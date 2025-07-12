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

        // Các thuộc tính trừu tượng mà lớp con phải định nghĩa
        protected abstract string SourceCollectionName { get; }
        protected abstract string MongoDatabaseName { get; }

        // Cấu hình khoảng thời gian xử lý tối đa cho mỗi lần chạy (tính bằng phút)
        protected virtual int MaxBatchIntervalInMinutes => 30; // In minutes

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

            try
            {
                var now = DateTime.UtcNow;
                var lastSuccessfulRun = LogManager.GetLastSuccessfulWatermark();

                // --- LOGIC MỚI: Xác định khoảng thời gian xử lý ---
                var potentialEndDate = lastSuccessfulRun.AddMinutes(MaxBatchIntervalInMinutes);
                var endDate = potentialEndDate < now ? potentialEndDate : now;

                // Kiểm tra an toàn: Nếu không có khoảng thời gian mới để xử lý, bỏ qua lần chạy này.
                if (endDate <= lastSuccessfulRun)
                {
                    var message = $"No new time window to process (end time <= start time). Skipping this run.";
                    context?.WriteLine(message);
                    Log.Information("[{JobName}] {Message}", SourceCollectionName, message);
                    return;
                }
                // --- KẾT THÚC LOGIC MỚI ---

                pendingFailedRecordIds = FailedRecordManager.GetPendingFailedRecordIds();

                // Ghi log với khoảng thời gian thực tế sẽ được xử lý
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

                if (pendingFailedRecordIds.Count != 0)
                {
                    FailedRecordManager.MarkRecordsAsResolved(pendingFailedRecordIds);
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
                }
                throw; // Ném lại lỗi để Hangfire đánh dấu job là 'Failed'
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
                Log.Information("[{JobName}] Adding {Count} failed records to the source query.", SourceCollectionName, failedIds.Count);
                var objectIds = failedIds.Select(id => new ObjectId(id)).ToList();
                var retryFilter = Builders<BsonDocument>.Filter.In("_id", objectIds);
                finalFilter = Builders<BsonDocument>.Filter.Or(watermarkFilter, retryFilter);
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
                            NotificationService.SendRecordErrorAsync(SourceCollectionName, recordId, exception).GetAwaiter().GetResult();
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
                    }
                }
            };
        }
    }
}