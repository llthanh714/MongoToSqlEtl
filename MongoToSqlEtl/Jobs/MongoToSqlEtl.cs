using ETLBox;
using ETLBox.DataFlow;
using ETLBox.MongoDb;
using Hangfire.Console;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoToSqlEtl.Managers;
using MongoToSqlEtl.Services;
using Serilog;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl.Jobs
{
    public abstract class EtlJob
    {
        protected readonly IConnectionManager SqlConnectionManager;
        protected readonly MongoClient MongoClient;
        protected readonly INotificationService NotificationService;
        protected readonly EtlLogManager LogManager;
        protected readonly EtlFailedRecordManager FailedRecordManager;

        // Danh sách để thu thập các ID bị lỗi trong lần chạy hiện tại
        protected List<string> CurrentRunFailedIds { get; } = [];
        // Object lock để đảm bảo an toàn luồng khi thêm vào danh sách lỗi
        private readonly object _failedIdsLock = new();

        protected abstract string SourceCollectionName { get; }
        protected abstract string MongoDatabaseName { get; }
        protected virtual int MaxBatchIntervalInMinutes => 30;

        protected EtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService)
        {
            SqlConnectionManager = sqlConnectionManager;
            MongoClient = mongoClient;
            NotificationService = notificationService;
            LogManager = new EtlLogManager(sqlConnectionManager, SourceCollectionName);
            FailedRecordManager = new EtlFailedRecordManager(sqlConnectionManager, SourceCollectionName);
        }

        protected abstract EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds, PerformContext? context);

        public async Task RunAsync(PerformContext? context)
        {
            int logId = 0;
            List<string> pendingFailedRecordIds = [];
            CurrentRunFailedIds.Clear(); // Xóa danh sách lỗi của lần chạy trước

            try
            {
                var now = DateTime.UtcNow;
                var lastSuccessfulRun = LogManager.GetLastSuccessfulWatermark();

                var potentialEndDate = lastSuccessfulRun.AddMinutes(MaxBatchIntervalInMinutes);
                var endDate = potentialEndDate < now ? potentialEndDate : now;

                if (endDate <= lastSuccessfulRun)
                {
                    var message = $"No new time window to process (end time <= start time). Skipping this run.";
                    context?.WriteLine(message);
                    Log.Information("[{JobName}] {Message}", SourceCollectionName, message);
                    return;
                }

                pendingFailedRecordIds = FailedRecordManager.GetPendingFailedRecordIds();
                logId = LogManager.StartNewLogEntry(lastSuccessfulRun, endDate);

                var pipeline = BuildPipeline(lastSuccessfulRun, endDate, pendingFailedRecordIds, context);

                context?.WriteLine($"Starting Network execution for job '{SourceCollectionName}'...");
                Log.Information("Starting Network execution for job '{JobName}'...", SourceCollectionName);

                await Network.ExecuteAsync(pipeline.Source);

                context?.WriteLine("Network execution has finished.");
                Log.Information("Network execution finished for job '{JobName}'.", SourceCollectionName);

                long totalSourceCount = pipeline.Source.ProgressCount;
                long successCount = pipeline.Destinations.Sum(d => d.ProgressCount);
                long failedCount = pipeline.ErrorDestination.ProgressCount;

                context?.WriteLine($"Summary -- Source: {totalSourceCount}, Successful: {successCount}, Failed: {failedCount}");
                LogManager.UpdateLogEntryOnSuccess(logId, totalSourceCount, successCount, failedCount);

                // Gửi một thông báo tổng hợp nếu có lỗi
                if (CurrentRunFailedIds.Count != 0)
                {
                    await NotificationService.SendFailedRecordsSummaryAsync(SourceCollectionName, CurrentRunFailedIds);
                }

                // FIX: DATA INTEGRITY: Only mark records as resolved if they were in the initial pending list
                // AND did not fail again in the current run.
                var successfullyRetriedIds = pendingFailedRecordIds.Except(CurrentRunFailedIds).ToList();
                if (successfullyRetriedIds.Count > 0)
                {
                    FailedRecordManager.MarkRecordsAsResolved(successfullyRetriedIds);
                }
            }
            catch (Exception ex)
            {
                context?.SetTextColor(ConsoleTextColor.Red);
                context?.WriteLine($"Job '{SourceCollectionName}' has failed with a critical error.");
                context?.WriteLine(ex.ToString());
                context?.ResetTextColor();

                if (logId > 0)
                {
                    LogManager.UpdateLogEntryOnFailure(logId, ex.ToString());
                    await NotificationService.SendFatalErrorAsync(SourceCollectionName, ex);
                }
                throw;
            }
        }

        protected virtual MongoDbSource<ExpandoObject> CreateMongoDbSource(DateTime startDate, DateTime endDate, List<string> failedIds)
        {
            var watermarkFilter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Gte("modifiedat", startDate),
                Builders<BsonDocument>.Filter.Lt("modifiedat", endDate)
            );

            FilterDefinition<BsonDocument> finalFilter;

            if (failedIds.Count != 0)
            {
                // FIX: FORMAT EXCEPTION: Validate that the ID is a valid ObjectId before trying to convert.
                // This prevents crashes if an invalid ID (like "INVALID_JSON_RECORD") is in the failed records table.
                var objectIds = failedIds
                    .Where(id => ObjectId.TryParse(id, out _))
                    .Select(id => new ObjectId(id))
                    .ToList();

                Log.Information("[{JobName}] Found {TotalCount} pending failed records, {ValidCount} are valid ObjectIds.",
                    SourceCollectionName, failedIds.Count, objectIds.Count);

                if (objectIds.Count > 0)
                {
                    var retryFilter = Builders<BsonDocument>.Filter.In("_id", objectIds);
                    finalFilter = Builders<BsonDocument>.Filter.Or(watermarkFilter, retryFilter);
                }
                else
                {
                    finalFilter = watermarkFilter;
                }
            }
            else
            {
                finalFilter = watermarkFilter;
            }

            Log.Information("[{JobName}] Fetching data from collection '{collection}' for time range: [{StartDate}, {EndDate}) and/or failed IDs.",
                SourceCollectionName, MongoDatabaseName, startDate, endDate);

            return new MongoDbSource<ExpandoObject>
            {
                DbClient = MongoClient,
                DatabaseName = MongoDatabaseName,
                CollectionName = SourceCollectionName,
                Filter = finalFilter,
                FindOptions = new FindOptions { BatchSize = 500 }
            };
        }

        protected virtual CustomDestination<ETLBoxError> CreateErrorLoggingDestination(PerformContext? context)
        {
            return new CustomDestination<ETLBoxError>
            {
                WriteAction = (error, rowIndex) =>
                {
                    // FIX: ROBUSTNESS: Add a top-level try-catch to prevent the error logging itself from crashing the app.
                    try
                    {
                        var exception = error.GetException();
                        var json = error.RecordAsJson;

                        if (string.IsNullOrEmpty(json))
                        {
                            Log.Error(exception, "Error record data (RecordAsJson) is null or empty.");
                            return;
                        }

                        void ProcessSingleRecord(IDictionary<string, object?> recordDict)
                        {
                            string? recordId = null;
                            if (recordDict != null && recordDict.TryGetValue("_id", out var idValue) && idValue != null)
                            {
                                recordId = idValue.ToString();
                            }

                            if (string.IsNullOrEmpty(recordId) || recordId == "[]")
                            {
                                Log.Warning(exception, "Could not find a valid ID in the error record to log. Data: {@ErrorRecord}", recordDict);
                            }
                            else
                            {
                                context?.WriteLine($"Data Row Error. ID: {recordId}. Error: {exception.Message}");
                                Log.Warning(exception, "Data Row Error. ID: {RecordId}, Data: {@ErrorRecord}", recordId, recordDict);

                                FailedRecordManager.LogFailedRecord(recordId, exception.ToString());

                                // Use lock to ensure thread safety when adding to the list
                                lock (_failedIdsLock)
                                {
                                    CurrentRunFailedIds.Add(recordId);
                                }
                            }
                        }

                        try
                        {
                            using var jsonDoc = JsonDocument.Parse(json);
                            if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                            {
                                Log.Warning(exception, "Error occurred on a data batch. Batch details: {Json}", json);
                                var records = JsonSerializer.Deserialize<List<ExpandoObject>>(json);
                                if (records != null)
                                {
                                    foreach (var record in records) ProcessSingleRecord(record);
                                }
                            }
                            else if (jsonDoc.RootElement.ValueKind == JsonValueKind.Object)
                            {
                                var record = JsonSerializer.Deserialize<ExpandoObject>(json);
                                if (record != null) ProcessSingleRecord(record);
                            }
                        }
                        catch (JsonException ex)
                        {
                            Log.Error(ex, "Could not parse error data from JSON. Raw JSON: {Json}", json);
                            FailedRecordManager.LogFailedRecord("INVALID_JSON_RECORD", $"JsonParseException: {ex.Message}. RawData: {json}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Fatal(ex, "A critical, unhandled exception occurred within the error logging action itself.");
                    }
                }
            };
        }
    }
}