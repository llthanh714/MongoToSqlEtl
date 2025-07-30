using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.MongoDb;
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
        protected virtual int MaxBatchIntervalInMinutes => 120;

        protected EtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService)
        {
            SqlConnectionManager = sqlConnectionManager;
            MongoClient = mongoClient;
            NotificationService = notificationService;
            LogManager = new EtlLogManager(sqlConnectionManager, SourceCollectionName);
            FailedRecordManager = new EtlFailedRecordManager(sqlConnectionManager, SourceCollectionName);
        }

        /// <summary>
        /// Builds the ETL pipeline for the job.
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="failedIds"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        protected abstract EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds, PerformContext? context);

        /// <summary>
        /// Executes the ETL job.
        /// This method is decorated with DisableConcurrentExecution to ensure that only one instance of this job
        /// runs at any given time. The timeout is a safeguard to prevent deadlocks if a job instance crashes.
        /// </summary>
        [DisableConcurrentExecution(timeoutInSeconds: 15 * 60)] // (15 minutes)
        public async Task RunAsync(PerformContext? context)
        {
            int logId = 0;
            List<string> pendingFailedRecordIds = [];

            // Re-initialize the bag for the new run.
            CurrentRunFailedIds = [];

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

                await TruncateStagingTablesAsync(context);

                var pipeline = BuildPipeline(lastSuccessfulRun, endDate, pendingFailedRecordIds, context);

                context?.WriteLine($"Starting Network execution for job '{SourceCollectionName}'...");
                Log.Information("Starting Network execution for job '{JobName}'...", SourceCollectionName);

                await Network.ExecuteAsync(pipeline.Source);

                // Check if the source has been executed by stored procedure
                if (!string.IsNullOrEmpty(pipeline.SqlStoredProcedureName))
                {
                    var mergeDataTask = new SqlTask($"EXEC {pipeline.SqlStoredProcedureName}")
                    {
                        ConnectionManager = SqlConnectionManager,
                        DisableLogging = true
                    };

                    await mergeDataTask.ExecuteNonQueryAsync();
                }

                context?.WriteLine("Network execution has finished.");
                Log.Information("Network execution finished for job '{JobName}'.", SourceCollectionName);

                long totalSourceCount = pipeline.Source.ProgressCount;
                long successCount = pipeline.Destinations.Sum(d => d.ProgressCount);
                long failedCount = pipeline.ErrorDestination.ProgressCount;

                context?.WriteLine($"Summary --> Source: {totalSourceCount}, Successful: {successCount}, Failed: {failedCount}");
                LogManager.UpdateLogEntryOnSuccess(logId, totalSourceCount, successCount, failedCount);

                var failedIdsList = CurrentRunFailedIds.ToList();
                if (failedIdsList.Count != 0)
                {
                    await NotificationService.SendFailedRecordsSummaryAsync(SourceCollectionName, failedIdsList);
                }

                var successfullyRetriedIds = pendingFailedRecordIds.Except(failedIdsList).ToList();
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

        /// <summary>
        /// Creates a MongoDB source for the ETL pipeline.
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="failedIds"></param>
        /// <returns></returns>
        protected virtual MongoDbSource<ExpandoObject> CreateMongoDbSource(DateTime startDate, DateTime endDate, List<string> failedIds)
        {
            var watermarkFilter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Gte("modifiedat", startDate),
                Builders<BsonDocument>.Filter.Lt("modifiedat", endDate)
            );

            FilterDefinition<BsonDocument> finalFilter;

            if (failedIds.Count != 0)
            {
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

        /// <summary>
        /// Creates a custom destination for logging errors that occur during the ETL process.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
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

                    // Lấy thông điệp lỗi gốc để ghi log
                    var reason = exception.ToString();

                    // Chuyển đổi JSON lỗi thành một danh sách các đối tượng.
                    // Dù là lỗi một bản ghi hay một lô, chúng ta đều xử lý như một danh sách.
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

                    // Ghi log lỗi cho TẤT CẢ các bản ghi trong dữ liệu bị lỗi.
                    // Lần chạy ETL tiếp theo sẽ tự động xử lý lại các ID này.
                    foreach (var recordDict in records)
                    {
                        if (recordDict != null && recordDict.TryGetValue("_id", out var idValue) && idValue != null)
                        {
                            string recordId = idValue.ToString()!;
                            if (!string.IsNullOrEmpty(recordId))
                            {
                                context?.WriteLine($"Data Row Error. ID: {recordId}. Reason: Batch write failed.");
                                // Log.Warning("Logging failed record ID: {RecordId} due to batch error.", recordId);

                                // Sử dụng FailedRecordManager để ghi nhận ID lỗi
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

        /// <summary>
        /// Executes the 'sp_truncate_staging_tables' stored procedure using a Table-Valued Parameter
        /// to clear a specific list of staging tables.
        /// </summary>
        protected async Task TruncateStagingTablesAsync(PerformContext? context)
        {
            var jobName = SourceCollectionName;
            var tablesToTruncate = StagingTables; // Lấy danh sách từ thuộc tính trừu tượng

            if (tablesToTruncate == null || tablesToTruncate.Count == 0)
            {
                Log.Information("[{JobName}] No specific staging tables defined to be cleared. Skipping truncation.", jobName);
                context?.WriteLine("No staging tables specified to clear. Skipping.");
                return;
            }

            context?.WriteLine($"Executing stored procedure to clear {tablesToTruncate.Count} specific staging table(s)...");
            Log.Information("[{JobName}] Executing 'sp_truncate_staging_tables' for tables: {TableNames}", jobName, string.Join(", ", tablesToTruncate));

            // Tạo một DataTable trong bộ nhớ để khớp với cấu trúc của UDTT
            var tableData = new DataTable("StringList");
            tableData.Columns.Add("Value", typeof(string));
            foreach (var tableName in tablesToTruncate)
            {
                tableData.Rows.Add(tableName);
            }

            try
            {
                // Sử dụng ADO.NET trực tiếp để có toàn quyền kiểm soát tham số TVP
                string connectionString = SqlConnectionManager.ConnectionString.Value;
                using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();
                using var cmd = new SqlCommand("dbo.sp_TruncateStagingTables", conn)
                {
                    CommandType = CommandType.StoredProcedure
                };

                // Tạo tham số và gán dữ liệu DataTable
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

        #region Transformation Logic Specific to PatientOrders

        /// <summary>
        /// Component CHỈ CÓ CHỨC NĂNG làm phẳng (flatten) một mảng con.
        /// Nó sẽ giữ lại TẤT CẢ các trường của các phần tử con.
        /// </summary>
        protected static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenComponent(
            string arrayFieldName,
            string foreignKeyName,
            string parentIdFieldName = "_id")
        {
            // Hàm được truyền vào RowMultiplication bây giờ là một hàm iterator.
            return new RowMultiplication<ExpandoObject, ExpandoObject>(
                parentRow => Flatten(parentRow, arrayFieldName, foreignKeyName, parentIdFieldName)
            );
        }

        /// <summary>
        /// Một iterator sử dụng 'yield return' để làm phẳng (flatten) một mảng con
        /// một cách hiệu quả về bộ nhớ.
        /// </summary>
        private static IEnumerable<ExpandoObject> Flatten(ExpandoObject parentRow, string arrayFieldName, string foreignKeyName, string parentIdFieldName)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;

            if (!parentAsDict.TryGetValue(parentIdFieldName, out var parentIdValue))
            {
                yield break;
            }
            var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;

            if (!parentAsDict.TryGetValue(arrayFieldName, out object? value) || value is not IEnumerable<object> items)
            {
                yield break;
            }

            // Lặp trực tiếp trên IEnumerable, không dùng ToList()
            foreach (var sourceItem in items)
            {
                if (sourceItem == null) continue;

                // Tạo bản sao độc lập của item con để đảm bảo an toàn luồng
                var itemAsExpando = (sourceItem is ExpandoObject expando)
                    ? expando
                    : ConvertToExando(sourceItem);

                if (itemAsExpando == null) continue;

                // Tạo một ExpandoObject mới để tránh thay đổi đối tượng gốc trong luồng dữ liệu
                var safeItemCopy = new ExpandoObject();
                var safeItemDict = (IDictionary<string, object?>)safeItemCopy;

                // Sao chép tất cả các thuộc tính từ item gốc sang bản sao an toàn
                foreach (var kvp in itemAsExpando)
                {
                    safeItemDict[kvp.Key] = kvp.Value;
                }

                // Thao tác ghi bây giờ hoàn toàn an toàn trên bản sao
                safeItemDict[foreignKeyName] = parentIdAsString;

                yield return safeItemCopy;
            }
        }

        /// <summary>
        /// Component CHỈ CÓ CHỨC NĂNG biến đổi và ánh xạ các trường của một đối tượng
        /// với các cột của bảng đích.
        /// </summary>
        protected static RowTransformation<ExpandoObject> CreateTransformAndMapComponent(
            ICollection<string> targetColumns,
            HashSet<string>? keepAsObjectFields = null)
        {
            // Chúng ta chỉ cần gọi lại logic đã có trong DataTransformer
            return new RowTransformation<ExpandoObject>(row =>
                DataTransformer.TransformObject(row, targetColumns, keepAsObjectFields)
            );
        }

        /// <summary>
        /// Convert một đối tượng thành ExpandoObject.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        protected static ExpandoObject? ConvertToExando(object obj)
        {
            try
            {
                // BsonDocument có phương thức .ToJson() để chuyển đổi hiệu quả.
                if (obj is BsonDocument bsonDoc)
                {
                    // Chuyển BsonDocument thành chuỗi JSON, sau đó deserialize thành ExpandoObject.
                    // Sử dụng các tùy chọn để đảm bảo định dạng JSON chuẩn, dễ đọc bởi System.Text.Json.
                    var json = bsonDoc.ToJson(new MongoDB.Bson.IO.JsonWriterSettings { OutputMode = MongoDB.Bson.IO.JsonOutputMode.RelaxedExtendedJson });
                    var expando = JsonSerializer.Deserialize<ExpandoObject>(json);
                    return expando;
                }

                // Đối với các loại object khác (ví dụ: các kiểu anonymous), serialize trực tiếp.
                var generalJson = JsonSerializer.Serialize(obj);
                var generalExpando = JsonSerializer.Deserialize<ExpandoObject>(generalJson);
                return generalExpando;
            }
            catch (Exception ex)
            {
                // Ghi lại log nếu có lỗi xảy ra trong quá trình chuyển đổi.
                Log.Warning(ex, "Could not convert object to ExpandoObject. Object Type: {ObjectType}", obj?.GetType().FullName ?? "null");
                return null;
            }
        }

        /// <summary>
        /// Component gộp: Vừa làm phẳng (flatten) một mảng con, vừa biến đổi (transform)
        /// và ánh xạ (map) các trường của kết quả theo các cột của bảng đích.
        /// Đây là sự kết hợp của CreateFlattenComponent và CreateTransformAndMapComponent.
        /// </summary>
        protected static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndTransformComponent(
            string arrayFieldName,
            string foreignKeyName,
            ICollection<string> targetColumns,
            string parentIdFieldName = "_id",
            HashSet<string>? keepAsObjectFields = null)
        {
            // Hàm được truyền vào RowMultiplication là một hàm iterator (sử dụng yield return)
            // để xử lý hiệu quả về bộ nhớ, tạo ra nhiều bản ghi con từ một bản ghi cha.
            return new RowMultiplication<ExpandoObject, ExpandoObject>(
                parentRow => FlattenAndTransform(
                    parentRow,
                    arrayFieldName,
                    foreignKeyName,
                    targetColumns,
                    parentIdFieldName,
                    keepAsObjectFields
                )
            );
        }

        /// <summary>
        /// Iterator thực hiện logic làm phẳng và biến đổi.
        /// </summary>
        private static IEnumerable<ExpandoObject> FlattenAndTransform(
            ExpandoObject parentRow,
            string arrayFieldName,
            string foreignKeyName,
            ICollection<string> targetColumns,
            string parentIdFieldName,
            HashSet<string>? keepAsObjectFields)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;

            // Lấy ID của đối tượng cha để gán làm khóa ngoại cho các đối tượng con.
            if (!parentAsDict.TryGetValue(parentIdFieldName, out var parentIdValue))
            {
                // Nếu không có ID cha, không thể tạo liên kết, bỏ qua.
                yield break;
            }
            var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;

            // Tìm và kiểm tra xem trường cần làm phẳng có phải là một danh sách hay không.
            if (!parentAsDict.TryGetValue(arrayFieldName, out object? value) || value is not IEnumerable<object> items)
            {
                // Nếu không phải danh sách, không có gì để làm phẳng.
                yield break;
            }

            // Lặp qua từng phần tử trong mảng con.
            foreach (var sourceItem in items)
            {
                if (sourceItem == null) continue;

                // Chuyển đổi phần tử con thành ExpandoObject nếu cần.
                var itemAsExpando = (sourceItem is ExpandoObject expando)
                    ? expando
                    : ConvertToExando(sourceItem);

                if (itemAsExpando == null) continue;

                var itemAsDict = (IDictionary<string, object?>)itemAsExpando;

                // Thêm khóa ngoại (ID của cha) vào đối tượng con.
                itemAsDict[foreignKeyName] = parentIdAsString;

                // Thực hiện biến đổi và ánh xạ đối tượng con đã có khóa ngoại
                // để khớp với cấu trúc của bảng đích trong SQL.
                var transformedItem = DataTransformer.TransformObject(itemAsExpando, targetColumns, keepAsObjectFields);

                // Trả về đối tượng đã được biến đổi hoàn chỉnh.
                yield return transformedItem;
            }
        }

        #endregion
    }
}