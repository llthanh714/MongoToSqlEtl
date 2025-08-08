using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire;
using Hangfire.Console;
using Hangfire.Server;
using Microsoft.Data.SqlClient;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Managers;
using MongoToSqlEtl.Services;
using Serilog;
using System.Collections.Concurrent;
using System.Data;
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

        protected ConcurrentBag<string> CurrentRunFailedIds { get; private set; } = [];
        protected abstract List<string> StagingTables { get; }
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

        protected abstract EtlPipeline BuildPipeline(List<ExpandoObject> batchData, PerformContext? context);

        [DisableConcurrentExecution(timeoutInSeconds: 15 * 60)]
        public async Task RunAsync(PerformContext? context, JobSettings jobSettings)
        {
            int logId = 0;
            try
            {
                // ✅ SỬA ĐỔI: Logic tính toán `recordsToSkip` được thêm vào ở đây.
                long recordsToSkip = 0;
                if (jobSettings.Backfill.Enabled)
                {
                    // Lấy tổng số bản ghi đã xử lý trong các lần backfill trước để biết cần skip bao nhiêu.
                    recordsToSkip = LogManager.GetTotalBackfillRecordsProcessed();
                }

                // ✅ SỬA ĐỔI: Truyền `recordsToSkip` (kiểu long) vào làm tham số đầu tiên.
                var (batchData, newWatermark) = await FetchNextBatchAsync(recordsToSkip, jobSettings.MaxRecordsPerJob, jobSettings.Backfill.Enabled);

                // Đoạn code kiểm tra batchData.Any() bị sai, sửa lại như sau:
                if (batchData.Count == 0)
                {
                    var message = jobSettings.Backfill.Enabled
                        ? "Backfill completed. No more records to process."
                        : "No new data to process.";
                    context?.WriteLine(message);
                    Log.Information("[{JobName}] {Message}", SourceCollectionName, message);
                    return;
                }

                // Lấy watermark cũ chỉ để phục vụ việc ghi log.
                var previousWatermark = LogManager.GetLastSuccessfulWatermark();
                logId = LogManager.StartNewLogEntry(previousWatermark, newWatermark);

                await TruncateStagingTablesAsync(context);

                // Chữ ký của BuildPipeline cũng cần được cập nhật để bỏ tham số không dùng đến
                var pipeline = BuildPipeline(batchData, context);

                context?.WriteLine($"Starting Network execution for job '{SourceCollectionName}' with {batchData.Count} records...");
                await Network.ExecuteAsync(pipeline.Source);

                if (!string.IsNullOrEmpty(pipeline.SqlStoredProcedureName))
                {
                    var mergeDataTask = new SqlTask($"EXEC {pipeline.SqlStoredProcedureName}") { ConnectionManager = SqlConnectionManager, DisableLogging = true };
                    await mergeDataTask.ExecuteNonQueryAsync();
                }

                context?.WriteLine("Network execution has finished.");
                long totalSourceCount = pipeline.Source.ProgressCount;
                long successCount = pipeline.Destinations.Sum(d => d.ProgressCount);
                long failedCount = 0; // Giả định không có lỗi, bạn có thể thay đổi nếu cần

                context?.WriteLine($"Summary --> Source: {totalSourceCount}, Successful (Total Processed): {successCount}, Failed: {failedCount}");
                LogManager.UpdateLogEntryOnSuccess(logId, totalSourceCount, successCount, failedCount);
            }
            catch (Exception ex)
            {
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

        /// <summary>
        /// ✅ SỬA ĐỔI: Chứa logic cốt lõi để tìm nạp dữ liệu theo 2 chế độ riêng biệt.
        /// </summary>
        protected virtual async Task<(List<ExpandoObject> Data, EtlWatermark NewWatermark)> FetchNextBatchAsync(long recordsToSkip, int batchSize, bool isBackfill)
        {
            var collection = MongoClient.GetDatabase(MongoDatabaseName).GetCollection<BsonDocument>(SourceCollectionName);
            var filterBuilder = Builders<BsonDocument>.Filter;
            FilterDefinition<BsonDocument> filter;
            SortDefinition<BsonDocument> sort;

            if (isBackfill)
            {
                Log.Information("[{JobName}] Running in Backfill mode using skip/limit. Skipping: {Skip}, Taking: {Take}", SourceCollectionName, recordsToSkip, batchSize);
                filter = filterBuilder.Empty; // Không cần filter
                sort = Builders<BsonDocument>.Sort.Ascending("_id"); // Vẫn sort để đảm bảo thứ tự nhất quán
            }
            else
            {
                var lastWatermark = LogManager.GetLastSuccessfulWatermark();
                Log.Information("[{JobName}] Running in Normal mode. Filtering by modifiedat > {Date}", SourceCollectionName, lastWatermark.LastModifiedAt);
                filter = filterBuilder.Gt("modifiedat", lastWatermark.LastModifiedAt);
                sort = Builders<BsonDocument>.Sort.Ascending("modifiedat");
            }

            var findQuery = collection.Find(filter).Sort(sort);
            if (isBackfill)
            {
                findQuery = findQuery.Skip((int)recordsToSkip);
            }
            var documents = await findQuery.Limit(batchSize).ToListAsync();

            if (documents.Count == 0)
            {
                // Trả về watermark rỗng để báo hiệu đã hết dữ liệu
                return ([], new EtlWatermark(DateTime.MinValue, null));
            }

            var lastDoc = documents.Last();
            var lastIdValue = lastDoc["_id"].IsObjectId ? lastDoc["_id"].AsObjectId.ToString() : lastDoc["_id"].AsString;
            var newWatermark = new EtlWatermark(lastDoc.Contains("modifiedat") ? lastDoc["modifiedat"].ToUniversalTime() : DateTime.UtcNow, lastIdValue);
            var expandoData = documents.Select(doc => ConvertToExando(doc)).Where(e => e != null).Cast<ExpandoObject>().ToList();

            Log.Information("[{JobName}] Fetched {Count} records. New watermark will be: {Watermark}", SourceCollectionName, expandoData.Count, newWatermark);
            return (expandoData, newWatermark);
        }

        #region Unchanged Methods
        protected virtual CustomDestination<ETLBoxError> CreateErrorLoggingDestination(PerformContext? context)
        {
            return new CustomDestination<ETLBoxError>((error, rowIndex) =>
            {
                try
                {
                    var exception = error.GetException();
                    var json = error.RecordAsJson;

                    if (string.IsNullOrEmpty(json))
                    {
                        Log.Error(exception, "Error record data (RecordAsJson) is null or empty.");
                        return;
                    }

                    var reason = exception.ToString();
                    var records = new List<IDictionary<string, object?>>();
                    using (var jsonDoc = JsonDocument.Parse(json))
                    {
                        if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                        {
                            var recordList = JsonSerializer.Deserialize<List<ExpandoObject>>(json);
                            if (recordList != null) records.AddRange(recordList);
                        }
                        else if (jsonDoc.RootElement.ValueKind == JsonValueKind.Object)
                        {
                            var singleRecord = JsonSerializer.Deserialize<ExpandoObject>(json);
                            if (singleRecord != null) records.Add(singleRecord);
                        }
                    }

                    foreach (var recordDict in records)
                    {
                        if (recordDict != null && recordDict.TryGetValue("_id", out var idValue) && idValue != null)
                        {
                            string recordId = idValue.ToString()!;
                            if (!string.IsNullOrEmpty(recordId))
                            {
                                context?.WriteLine($"Data Row Error. ID: {recordId}. Reason: Batch write failed.");
                                FailedRecordManager.LogFailedRecord(recordId, reason);
                                CurrentRunFailedIds.Add(recordId);
                            }
                        }
                        else
                        {
                            Log.Warning("Could not find a valid '_id' in the error record to log. Data: {@ErrorRecord}", recordDict);
                        }
                    }
                }
                catch (JsonException ex)
                {
                    Log.Error(ex, "Could not parse error data from JSON. Raw JSON: {Json}", error.RecordAsJson);
                    FailedRecordManager.LogFailedRecord("INVALID_JSON_RECORD", $"JsonParseException: {ex.Message}. RawData: {error.RecordAsJson}");
                }
                catch (Exception ex)
                {
                    Log.Fatal(ex, "A critical, unhandled exception occurred within the error logging action itself.");
                }
            });
        }
        protected async Task TruncateStagingTablesAsync(PerformContext? context)
        {
            var jobName = SourceCollectionName;
            var tablesToTruncate = StagingTables;

            if (tablesToTruncate == null || tablesToTruncate.Count == 0)
            {
                Log.Information("[{JobName}] No specific staging tables defined to be cleared. Skipping truncation.", jobName);
                context?.WriteLine("No staging tables specified to clear. Skipping.");
                return;
            }

            context?.WriteLine($"Executing stored procedure to clear {tablesToTruncate.Count} specific staging table(s)...");
            Log.Information("[{JobName}] Executing 'sp_truncate_staging_tables' for tables: {TableNames}", jobName, string.Join(", ", tablesToTruncate));

            var tableData = new DataTable("StringList");
            tableData.Columns.Add("Value", typeof(string));
            foreach (var tableName in tablesToTruncate)
            {
                tableData.Rows.Add(tableName);
            }

            try
            {
                string connectionString = SqlConnectionManager.ConnectionString.Value;
                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();
                using var cmd = new SqlCommand("dbo.sp_TruncateStagingTables", conn)
                {
                    CommandType = CommandType.StoredProcedure
                };
                var tvpParam = cmd.Parameters.AddWithValue("@TableNames", tableData);
                tvpParam.SqlDbType = SqlDbType.Structured;
                tvpParam.TypeName = "dbo.udtt_StringList";

                await cmd.ExecuteNonQueryAsync();

                context?.WriteLine("Stored procedure executed successfully. Specified staging tables are cleared.");
                Log.Information("[{JobName}] Successfully cleared specified staging tables.", jobName);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[{JobName}] A critical error occurred while executing 'sp_truncate_staging_tables' with TVP.", jobName);
                context?.SetTextColor(ConsoleTextColor.Red);
                context?.WriteLine($"Error: Failed to execute stored procedure to clear staging tables. The ETL process will be aborted.");
                context?.ResetTextColor();
                throw new Exception("Failed to truncate staging tables via stored procedure, aborting job execution.", ex);
            }
        }
        protected static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenComponent(string arrayFieldName, string foreignKeyName, string parentIdFieldName = "_id")
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => Flatten(parentRow, arrayFieldName, foreignKeyName, parentIdFieldName));
        }
        private static IEnumerable<ExpandoObject> Flatten(ExpandoObject parentRow, string arrayFieldName, string foreignKeyName, string parentIdFieldName)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            if (!parentAsDict.TryGetValue(parentIdFieldName, out var parentIdValue)) yield break;
            var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;
            if (!parentAsDict.TryGetValue(arrayFieldName, out object? value) || value == null) yield break;
            var itemsAsList = new List<object>();
            if (value is IEnumerable<object> list && value is not string) { itemsAsList.AddRange(list); } else { itemsAsList.Add(value); }
            foreach (var sourceItem in itemsAsList)
            {
                if (sourceItem == null) continue;
                var itemAsExpando = (sourceItem is ExpandoObject expando) ? expando : ConvertToExando(sourceItem);
                if (itemAsExpando == null) continue;
                var safeItemCopy = new ExpandoObject();
                var safeItemDict = (IDictionary<string, object?>)safeItemCopy;
                foreach (var kvp in itemAsExpando) { safeItemDict[kvp.Key] = kvp.Value; }
                safeItemDict[foreignKeyName] = parentIdAsString;
                yield return safeItemCopy;
            }
        }
        protected static RowTransformation<ExpandoObject> CreateTransformAndMapComponent(ICollection<string> targetColumns, HashSet<string>? keepAsObjectFields = null)
        {
            return new RowTransformation<ExpandoObject>(row => DataTransformer.TransformObject(row, targetColumns, keepAsObjectFields));
        }
        protected static ExpandoObject? ConvertToExando(object obj)
        {
            try
            {
                if (obj is BsonDocument bsonDoc)
                {
                    var json = bsonDoc.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
                    return JsonSerializer.Deserialize<ExpandoObject>(json);
                }

                if (obj is JsonElement jsonElem)
                {
                    // Lấy văn bản JSON thô từ JsonElement và deserialize trực tiếp từ đó.
                    var rawJson = jsonElem.GetRawText();

                    // Nếu chuỗi JSON rỗng hoặc không phải là một đối tượng, trả về null.
                    if (string.IsNullOrWhiteSpace(rawJson) || !rawJson.Trim().StartsWith('{'))
                    {
                        return null;
                    }

                    return JsonSerializer.Deserialize<ExpandoObject>(rawJson);
                }

                // Nhánh dự phòng cho các kiểu đối tượng khác.
                var generalJson = JsonSerializer.Serialize(obj);
                var generalExpando = JsonSerializer.Deserialize<ExpandoObject>(generalJson);
                return generalExpando;
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Could not convert object to ExpandoObject. Object Type: {ObjectType}, Object Data: {@Object}", obj?.GetType().FullName ?? "null", obj);
                return null;
            }
        }
        protected static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndTransformComponent(string arrayFieldName, string foreignKeyName, ICollection<string> targetColumns, string parentIdFieldName = "_id", HashSet<string>? keepAsObjectFields = null)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => FlattenAndTransform(parentRow, arrayFieldName, foreignKeyName, targetColumns, parentIdFieldName, keepAsObjectFields));
        }
        private static IEnumerable<ExpandoObject> FlattenAndTransform(ExpandoObject parentRow, string arrayFieldName, string foreignKeyName, ICollection<string> targetColumns, string parentIdFieldName, HashSet<string>? keepAsObjectFields)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            if (!parentAsDict.TryGetValue(parentIdFieldName, out var parentIdValue)) yield break;
            var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;
            if (!parentAsDict.TryGetValue(arrayFieldName, out object? value) || value == null) yield break;
            var itemsAsList = new List<object>();
            if (value is IEnumerable<object> list && value is not string) { itemsAsList.AddRange(list); } else { itemsAsList.Add(value); }
            foreach (var sourceItem in itemsAsList)
            {
                if (sourceItem == null) continue;
                var itemAsExpando = (sourceItem is ExpandoObject expando) ? expando : ConvertToExando(sourceItem);
                if (itemAsExpando == null) continue;
                var itemAsDict = (IDictionary<string, object?>)itemAsExpando;
                itemAsDict[foreignKeyName] = parentIdAsString;
                var transformedItem = DataTransformer.TransformObject(itemAsExpando, targetColumns, keepAsObjectFields);
                yield return transformedItem;
            }
        }
        #endregion
    }
}