using ETLBox;
using ETLBox.DataFlow;
using ETLBox.MongoDb;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoToSqlEtl.Managers;
using Serilog;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// Lớp cơ sở trừu tượng định nghĩa khung sườn cho một tiến trình ETL.
    /// </summary>
    public abstract class EtlJob
    {
        protected readonly IConnectionManager SqlConnectionManager;
        protected readonly MongoClient MongoClient;
        protected readonly EtlLogManager LogManager;
        protected readonly EtlFailedRecordManager FailedRecordManager;

        // Các thuộc tính trừu tượng mà lớp con phải định nghĩa
        protected abstract string SourceCollectionName { get; }
        protected abstract string MongoDatabaseName { get; }

        protected EtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient)
        {
            SqlConnectionManager = sqlConnectionManager;
            MongoClient = mongoClient;
            LogManager = new EtlLogManager(sqlConnectionManager, SourceCollectionName);
            FailedRecordManager = new EtlFailedRecordManager(sqlConnectionManager, SourceCollectionName);
        }

        /// <summary>
        /// Phương thức trừu tượng mà lớp con phải triển khai để xây dựng pipeline cụ thể.
        /// </summary>
        protected abstract EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds);

        /// <summary>
        /// Phương thức mẫu (Template Method) điều phối toàn bộ quá trình chạy ETL.
        /// </summary>
        public async Task RunAsync()
        {
            int logId = 0;
            List<string> pendingFailedRecordIds = [];

            try
            {
                var currentRunStartTime = DateTime.UtcNow;
                var lastSuccessfulRun = LogManager.GetLastSuccessfulWatermark();
                pendingFailedRecordIds = FailedRecordManager.GetPendingFailedRecordIds();

                logId = LogManager.StartNewLogEntry(lastSuccessfulRun, currentRunStartTime);

                // 1. Build the pipeline by passing parameters
                var pipeline = BuildPipeline(lastSuccessfulRun, currentRunStartTime, pendingFailedRecordIds);

                // 2. Execute the pipeline
                Log.Information("Bắt đầu thực thi Network cho job '{JobName}'...", SourceCollectionName);
                await Network.ExecuteAsync(pipeline.Source);
                Log.Information("Network đã thực thi xong cho job '{JobName}'.", SourceCollectionName);

                // 3. Collect stats AFTER execution
                long totalSourceCount = pipeline.Source.ProgressCount;
                long successCount = pipeline.Destinations.Sum(d => d.ProgressCount);
                long failedCount = pipeline.ErrorDestination.ProgressCount;

                // 4. Update log with correct stats
                LogManager.UpdateLogEntryOnSuccess(logId, totalSourceCount, successCount, failedCount);

                // Mark resolved records
                if (pendingFailedRecordIds.Count != 0)
                {
                    FailedRecordManager.MarkRecordsAsResolved(pendingFailedRecordIds);
                }
            }
            catch (Exception ex)
            {
                if (logId > 0)
                {
                    LogManager.UpdateLogEntryOnFailure(logId, ex.ToString());
                }
                throw new Exception($"Job '{SourceCollectionName}' failed.", ex);
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
                Log.Information("[{JobName}] Thêm {Count} bản ghi bị lỗi vào truy vấn nguồn.", SourceCollectionName, failedIds.Count);
                var objectIds = failedIds.Select(id => new ObjectId(id)).ToList();
                var retryFilter = Builders<BsonDocument>.Filter.In("_id", objectIds);
                finalFilter = Builders<BsonDocument>.Filter.Or(watermarkFilter, retryFilter);
            }
            else
            {
                finalFilter = watermarkFilter;
            }

            Log.Information("[{JobName}] Lấy dữ liệu từ collection '{collection}' trong khoảng: [{StartDate}, {EndDate}) và/hoặc các ID lỗi.",
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

        protected virtual CustomDestination<ETLBoxError> CreateErrorLoggingDestination()
        {
            // Logic xử lý và ghi log lỗi đã được tối ưu
            // (Giữ nguyên từ phiên bản trước)
            return new CustomDestination<ETLBoxError>
            {
                WriteAction = (error, rowIndex) =>
                {
                    var exception = error.GetException();
                    var json = error.RecordAsJson;

                    if (string.IsNullOrEmpty(json))
                    {
                        Log.Error(exception, "Dữ liệu lỗi (RecordAsJson) bị rỗng hoặc null.");
                        return;
                    }

                    // Helper function to process a single record dictionary
                    void ProcessSingleRecord(IDictionary<string, object?> recordDict)
                    {
                        string? recordId = null;

                        if (recordDict != null && recordDict.TryGetValue("_id", out var idValue) && idValue != null)
                        {
                            recordId = idValue.ToString();
                        }

                        // Check for invalid ID representations, including "[]" for empty collections.
                        if (string.IsNullOrEmpty(recordId) || recordId == "[]")
                        {
                            Log.Warning(exception, "Không tìm thấy ID hợp lệ trong bản ghi lỗi để ghi nhận. Data: {@ErrorRecord}", recordDict);
                        }
                        else
                        {
                            Log.Warning(exception, "Lỗi Dòng Dữ Liệu. ID: {RecordId}, Data: {@ErrorRecord}", recordId, recordDict);
                            FailedRecordManager.LogFailedRecord(recordId, exception.ToString());
                        }
                    }

                    try
                    {
                        // Use JsonDocument to inspect the JSON without fully deserializing into specific types yet
                        using var jsonDoc = JsonDocument.Parse(json);

                        if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                        {
                            // It's a batch error
                            Log.Warning(exception, "Lỗi xảy ra trên một batch dữ liệu. Chi tiết batch: {Json}", json);
                            var records = JsonSerializer.Deserialize<List<ExpandoObject>>(json);
                            if (records != null)
                            {
                                foreach (var record in records)
                                {
                                    ProcessSingleRecord(record);
                                }
                            }
                        }
                        else if (jsonDoc.RootElement.ValueKind == JsonValueKind.Object)
                        {
                            // It's a single record error
                            var record = JsonSerializer.Deserialize<ExpandoObject>(json);
                            if (record != null)
                            {
                                ProcessSingleRecord(record);
                            }
                        }
                        else
                        {
                            // The JSON is valid but is a primitive type (string, number, etc.), which is unexpected.
                            Log.Error(exception, "Dữ liệu lỗi không phải là một đối tượng JSON hoặc mảng. Raw JSON: {Json}", json);
                        }
                    }
                    catch (JsonException ex)
                    {
                        // If parsing fails completely, log the raw JSON
                        Log.Error(ex, "Không thể phân tích dữ liệu lỗi từ JSON. Raw JSON: {Json}", json);
                    }
                }
            };
        }
    }
}
