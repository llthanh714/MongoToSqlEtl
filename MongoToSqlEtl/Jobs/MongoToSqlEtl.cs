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

        protected abstract string SourceCollectionName { get; }
        protected abstract string MongoDatabaseName { get; }

        protected EtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService)
        {
            SqlConnectionManager = sqlConnectionManager;
            MongoClient = mongoClient;
            NotificationService = notificationService;
            LogManager = new EtlLogManager(sqlConnectionManager, SourceCollectionName);
            FailedRecordManager = new EtlFailedRecordManager(sqlConnectionManager, SourceCollectionName);
        }

        protected abstract EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds, PerformContext? context);

        // CẬP NHẬT: Thêm tham số PerformContext
        public async Task RunAsync(PerformContext? context)
        {
            int logId = 0;
            List<string> pendingFailedRecordIds = [];

            try
            {
                var currentRunStartTime = DateTime.UtcNow;
                var lastSuccessfulRun = LogManager.GetLastSuccessfulWatermark();
                pendingFailedRecordIds = FailedRecordManager.GetPendingFailedRecordIds();

                logId = LogManager.StartNewLogEntry(lastSuccessfulRun, currentRunStartTime);

                var pipeline = BuildPipeline(lastSuccessfulRun, currentRunStartTime, pendingFailedRecordIds, context);

                context?.WriteLine($"Starting Network execution for the job '{SourceCollectionName}'...");
                Log.Information("Starting Network execution for job '{JobName}'...", SourceCollectionName);

                await Network.ExecuteAsync(pipeline.Source);

                context?.WriteLine("The network execution is complete.");
                Log.Information("The network has finished executing for the job '{JobName}'.", SourceCollectionName);

                long totalSourceCount = pipeline.Source.ProgressCount;
                long successCount = pipeline.Destinations.Sum(d => d.ProgressCount);
                long failedCount = pipeline.ErrorDestination.ProgressCount;

                context?.WriteLine($"Summary --> Sources: {totalSourceCount}, Successful: {successCount}, Failed: {failedCount}");
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
                Log.Information("[{JobName}] Added {Count} failed records to the source query.", SourceCollectionName, failedIds.Count);
                var objectIds = failedIds.Select(id => new ObjectId(id)).ToList();
                var retryFilter = Builders<BsonDocument>.Filter.In("_id", objectIds);
                finalFilter = Builders<BsonDocument>.Filter.Or(watermarkFilter, retryFilter);
            }
            else
            {
                finalFilter = watermarkFilter;
            }

            Log.Information("[{JobName}] Retrieving data from collection '{collection}' in the range: [{StartDate}, {EndDate}) and/or by error IDs.",
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

        // CẬP NHẬT: Thêm tham số PerformContext
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
                        Log.Error(exception, "RecordAsJson is missing or contains no data.");
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
                            Log.Warning(exception, "No valid identifier detected in the error record for tracking. Data: {@ErrorRecord}", recordDict);
                        }
                        else
                        {
                            // Ghi log vào cả Serilog và Hangfire Dashboard
                            context?.WriteLine($"Error in data row. ID: {recordId}. Details: {exception.Message}");
                            Log.Warning(exception, "Record Error. ID: {RecordId}, Data: {@ErrorRecord}", recordId, recordDict);

                            FailedRecordManager.LogFailedRecord(recordId, exception.ToString());
                            NotificationService.SendRecordErrorAsync(SourceCollectionName, recordId, exception).GetAwaiter().GetResult();
                        }
                    }

                    try
                    {
                        using var jsonDoc = JsonDocument.Parse(json);
                        if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                        {
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
                        Log.Error(ex, "Unable to parse error data from JSON. Raw JSON: {Json}", json);
                    }
                }
            };
        }
    }
}
