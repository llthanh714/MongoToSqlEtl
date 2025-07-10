using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.MongoDb;
using ETLBox.SqlServer;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;
using System.Collections;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl
{
    #region Manager Classes
    public class EtlLogManager(IConnectionManager connectionManager, string sourceCollectionName)
    {
        private static readonly DateTime DefaultStartDate = new(2025, 7, 10, 0, 0, 0, DateTimeKind.Utc);

        public DateTime GetLastSuccessfulWatermark()
        {
            try
            {
                var sql = $@"
                    SELECT TOP 1 WatermarkEndTimeUtc FROM EtlExecutionLog
                    WHERE SourceCollectionName = '{sourceCollectionName}' AND Status = 'Succeeded'
                    ORDER BY ExecutionStartTimeUtc DESC";
                var result = SqlTask.ExecuteScalar(connectionManager, sql);
                if (result != null && result != DBNull.Value)
                {
                    // CẬP NHẬT QUAN TRỌNG: Chỉ định rõ DateTime đọc từ DB là giờ UTC.
                    var lastRun = DateTime.SpecifyKind((DateTime)result, DateTimeKind.Utc);
                    Log.Information("[LogManager] Found last successful watermark: {LastRun}", lastRun);
                    return lastRun;
                }
            }
            catch (Exception ex) { Log.Warning(ex, "[LogManager] Could not query EtlExecutionLog table."); }
            Log.Information("[LogManager] No watermark found, using default: {DefaultDate}", DefaultStartDate);
            return DefaultStartDate;
        }

        public int StartNewLogEntry(DateTime watermarkStart, DateTime watermarkEnd)
        {
            var sql = $@"
                INSERT INTO EtlExecutionLog (SourceCollectionName, ExecutionStartTimeUtc, WatermarkStartTimeUtc, WatermarkEndTimeUtc, Status)
                VALUES ('{sourceCollectionName}', GETUTCDATE(), '{watermarkStart:o}', '{watermarkEnd:o}', 'Started');
                SELECT SCOPE_IDENTITY();";
            var logId = Convert.ToInt32(SqlTask.ExecuteScalar(connectionManager, sql));
            Log.Information("[LogManager] Created new ETL log entry with ID: {LogId}", logId);
            return logId;
        }

        public void UpdateLogEntryOnSuccess(int logId, long sourceCount, long successCount, long failedCount)
        {
            var sql = $@"UPDATE EtlExecutionLog SET ExecutionEndTimeUtc = GETUTCDATE(), Status = 'Succeeded',
                SourceRecordCount = {sourceCount}, SuccessRecordCount = {successCount}, FailedRecordCount = {failedCount}, ErrorMessage = NULL
                WHERE Id = {logId};";
            SqlTask.ExecuteNonQuery(connectionManager, sql);
            Log.Information("[LogManager] Updated log entry ID: {LogId} to 'Succeeded'.", logId);
        }

        public void UpdateLogEntryOnFailure(int logId, string errorMessage)
        {
            var sanitizedErrorMessage = errorMessage.Replace("'", "''");
            var sql = $@"UPDATE EtlExecutionLog SET ExecutionEndTimeUtc = GETUTCDATE(), Status = 'Failed', ErrorMessage = '{sanitizedErrorMessage}'
                WHERE Id = {logId};";
            SqlTask.ExecuteNonQuery(connectionManager, sql);
            Log.Error("[LogManager] Updated log entry ID: {LogId} to 'Failed'.", logId);
        }
    }

    public class EtlFailedRecordManager(IConnectionManager connectionManager, string sourceCollectionName)
    {
        public List<string> GetPendingFailedRecordIds()
        {
            var ids = new List<string>();
            try
            {
                var sql = $"SELECT FailedRecordId FROM EtlFailedRecords WHERE SourceCollectionName = '{sourceCollectionName}' AND Status = 'Pending'";
                var task = new SqlTask(sql)
                {
                    ConnectionManager = connectionManager,
                    Actions = [id =>
                    {
                        if (id != null)
                        {
                            ids.Add(id.ToString()!);
                        }
                    }]
                };
                task.ExecuteReader();
                Log.Information("[FailedManager] Found {Count} pending failed records to retry.", ids.Count);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[FailedManager] Could not query EtlFailedRecords table.");
            }
            return ids;
        }

        public void LogFailedRecord(string recordId, string errorMessage)
        {
            var sanitizedErrorMessage = errorMessage.Replace("'", "''");
            var sql = $@"
                MERGE EtlFailedRecords AS target
                USING (SELECT '{sourceCollectionName}' AS SourceCollectionName, '{recordId}' AS FailedRecordId) AS source
                ON (target.SourceCollectionName = source.SourceCollectionName AND target.FailedRecordId = source.FailedRecordId)
                WHEN MATCHED THEN
                    UPDATE SET
                        Status = 'Pending',
                        ErrorMessage = '{sanitizedErrorMessage}',
                        LoggedAtUtc = GETUTCDATE(),
                        ResolvedAtUtc = NULL
                WHEN NOT MATCHED THEN
                    INSERT (SourceCollectionName, FailedRecordId, ErrorMessage, Status)
                    VALUES (source.SourceCollectionName, source.FailedRecordId, '{sanitizedErrorMessage}', 'Pending');";
            try
            {
                SqlTask.ExecuteNonQuery(connectionManager, sql);
            }
            catch (Exception ex) { Log.Error(ex, "[FailedManager] Could not log failed record ID: {RecordId}", recordId); }
        }

        public void MarkRecordsAsResolved(List<string> recordIds)
        {
            if (recordIds.Count == 0) return;

            var formattedIds = string.Join(",", recordIds.Select(id => $"'{id}'"));
            var sql = $@"
                UPDATE EtlFailedRecords
                SET Status = 'Resolved',
                    ResolvedAtUtc = GETUTCDATE()
                WHERE SourceCollectionName = '{sourceCollectionName}'
                  AND FailedRecordId IN ({formattedIds})
                  AND Status = 'Pending';";
            try
            {
                int count = SqlTask.ExecuteNonQuery(connectionManager, sql);
                Log.Information("[FailedManager] Marked {Count} previously pending records as 'Resolved'.", count);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[FailedManager] Could not mark records as resolved.");
            }
        }
    }
    #endregion

    public class Program
    {
        public static async Task Main()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug().Enrich.FromLogContext().WriteTo.Console()
                .WriteTo.File("logs/etl-log-.txt", rollingInterval: RollingInterval.Day,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            try
            {
                Log.Information("--- BẮT ĐẦU PHIÊN LÀM VIỆC ETL ---");
                await RunEtlProcess();
                Log.Information("--- KẾT THÚC PHIÊN LÀM VIỆC ETL THÀNH CÔNG ---");
            }
            catch (Exception ex) { Log.Fatal(ex, "Ứng dụng ETL đã gặp lỗi nghiêm trọng và bị dừng lại."); throw; }
            finally { await Log.CloseAndFlushAsync(); }
        }

        private static async Task RunEtlProcess()
        {
            var config = LoadConfiguration();
            var sqlConnectionManager = CreateSqlConnectionManager(config);
            var mongoClient = CreateMongoDbClient(config);
            const string sourceCollection = "patientorders";

            var logManager = new EtlLogManager(sqlConnectionManager, sourceCollection);
            var failedRecordManager = new EtlFailedRecordManager(sqlConnectionManager, sourceCollection);
            int logId = 0;
            List<string> pendingFailedRecordIds = [];

            try
            {
                var currentRunStartTime = DateTime.UtcNow;
                var lastSuccessfulRun = logManager.GetLastSuccessfulWatermark();
                pendingFailedRecordIds = failedRecordManager.GetPendingFailedRecordIds();

                logId = logManager.StartNewLogEntry(lastSuccessfulRun, currentRunStartTime);

                var patientordersDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorders");
                var patientorderitemsDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorderitems");
                var dispensebatchdetailDef = TableDefinition.FromTableName(sqlConnectionManager, "dispensebatchdetail");

                var source = CreateMongoDbSource(mongoClient, sourceCollection, lastSuccessfulRun, currentRunStartTime, pendingFailedRecordIds);
                var logErrors = CreateErrorLoggingDestination(failedRecordManager);

                var transformPatientOrders = CreateTransformComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
                var flattenAndNormalizeOrderItems = CreateFlattenAndNormalizeOrderItems();
                var transformOrderItemForSql = CreateTransformComponent([.. patientorderitemsDef.Columns.Select(c => c.Name)]);
                var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation([.. dispensebatchdetailDef.Columns.Select(c => c.Name)]);

                var destPatientOrders = CreateDbMergeDestination(sqlConnectionManager, "patientorders");
                var destPatientOrderItems = CreateDbMergeDestination(sqlConnectionManager, "patientorderitems");
                var destDispenseBatchDetail = CreateDbMergeDestination(sqlConnectionManager, "dispensebatchdetail");

                BuildAndLinkPipeline(source, logErrors, transformPatientOrders, flattenAndNormalizeOrderItems, transformOrderItemForSql, flattenDispenseBatchDetails, destPatientOrders, destPatientOrderItems, destDispenseBatchDetail);

                Log.Information("Bắt đầu thực thi Network...");
                await Network.ExecuteAsync(source);
                Log.Information("Network đã thực thi xong.");

                long successCount = destPatientOrders.ProgressCount + destPatientOrderItems.ProgressCount + destDispenseBatchDetail.ProgressCount;
                logManager.UpdateLogEntryOnSuccess(logId, source.ProgressCount, successCount, logErrors.ProgressCount);

                // Sau khi chạy thành công, đánh dấu các bản ghi lỗi đã được đưa vào xử lý là 'Resolved'.
                if (pendingFailedRecordIds.Count != 0)
                {
                    failedRecordManager.MarkRecordsAsResolved(pendingFailedRecordIds);
                }
            }
            catch (Exception ex)
            {
                if (logId > 0) { logManager.UpdateLogEntryOnFailure(logId, ex.ToString()); }
                throw;
            }
        }

        #region STEP 1: Configuration and Connection Setup
        private static IConfiguration LoadConfiguration() => new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        private static SqlConnectionManager CreateSqlConnectionManager(IConfiguration config)
        {
            string? cs = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("Connection string 'SqlServer' not found.");
            return new SqlConnectionManager(cs);
        }
        private static MongoClient CreateMongoDbClient(IConfiguration config)
        {
            string? cs = config.GetConnectionString("MongoDb") ?? throw new InvalidOperationException("Connection string 'MongoDb' not found.");
            return new MongoClient(cs);
        }
        #endregion

        #region STEP 2: ETL Component Creation
        private static MongoDbSource<ExpandoObject> CreateMongoDbSource(MongoClient client, string collectionName, DateTime startDate, DateTime endDate, List<string> failedIds)
        {
            // Filter for the new/updated data within the time window
            var watermarkFilter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Gte("modifiedat", startDate),
                Builders<BsonDocument>.Filter.Lt("modifiedat", endDate)
            );

            FilterDefinition<BsonDocument> finalFilter;

            // If there are records to retry, create a more complex $or filter
            if (failedIds.Count != 0)
            {
                Log.Information("Thêm {Count} bản ghi bị lỗi vào truy vấn nguồn.", failedIds.Count);

                // Filter for the failed records
                var objectIds = failedIds.Select(id => new ObjectId(id)).ToList();
                var retryFilter = Builders<BsonDocument>.Filter.In("_id", objectIds);

                // Combine the filters: (new data) OR (failed data)
                finalFilter = Builders<BsonDocument>.Filter.Or(watermarkFilter, retryFilter);
            }
            else
            {
                // If there are no records to retry, use only the simple time window filter
                finalFilter = watermarkFilter;
            }

            Log.Information("Lấy dữ liệu từ MongoDB collection '{collection}' trong khoảng: [{StartDate}, {EndDate}) và/hoặc các ID lỗi.", collectionName, startDate, endDate);
            return new MongoDbSource<ExpandoObject> { DbClient = client, DatabaseName = "arcusairdb", CollectionName = collectionName, Filter = finalFilter, FindOptions = new FindOptions { BatchSize = 500 } };
        }

        private static DbMerge<ExpandoObject> CreateDbMergeDestination(IConnectionManager conn, string tableName)
        {
            return new DbMerge<ExpandoObject>(conn, tableName) { MergeMode = MergeMode.InsertsAndUpdates, IdColumns = [new IdColumn { IdPropertyName = "_id" }], BatchSize = 500 };
        }

        private static RowTransformation<ExpandoObject> CreateTransformComponent(HashSet<string> targetColumns)
        {
            return new RowTransformation<ExpandoObject>(row => TransformObject(row, targetColumns));
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndNormalizeOrderItems()
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(FlattenAndNormalizeItems);
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateDispenseBatchDetailsTransformation(HashSet<string> cols)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(itemRow => FlattenAndTransformDispenseBatchDetail(itemRow, cols));
        }

        private static CustomDestination<ETLBoxError> CreateErrorLoggingDestination(EtlFailedRecordManager failedRecordManager)
        {
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
                            failedRecordManager.LogFailedRecord(recordId, exception.ToString());
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
        #endregion

        #region STEP 3 & 4: Logic and Linking (No changes from previous version)
        private static IEnumerable<ExpandoObject> FlattenAndNormalizeItems(ExpandoObject parentRow)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            if (parentAsDict.TryGetValue("_id", out var parentId) && parentId is ObjectId poid)
            {
                parentAsDict["_id"] = poid.ToString();
            }

            if (parentAsDict.TryGetValue("patientorderitems", out object? value) && value is IEnumerable<object> orderItems)
            {
                foreach (object? sourceItem in orderItems)
                {
                    if (sourceItem == null) continue;
                    var newItem = new ExpandoObject();
                    var newItemDict = (IDictionary<string, object?>)newItem;
                    var sourceItemDict = (IDictionary<string, object?>)sourceItem;
                    foreach (var key in sourceItemDict.Keys)
                    {
                        MapProperty(sourceItemDict, newItemDict, key);
                    }
                    newItemDict["patientordersuid"] = parentAsDict["_id"];
                    yield return newItem;
                }
            }
        }

        private static ExpandoObject TransformObject(ExpandoObject sourceRow, ICollection<string> targetColumns)
        {
            var sourceAsDict = (IDictionary<string, object?>)sourceRow;
            var targetDict = (IDictionary<string, object?>)new ExpandoObject();
            foreach (var columnName in targetColumns)
            {
                MapProperty(sourceAsDict, targetDict, columnName);
            }
            if (sourceAsDict.ContainsKey("_id") && !targetDict.ContainsKey("_id")) MapProperty(sourceAsDict, targetDict, "_id");
            if (sourceAsDict.ContainsKey("patientordersuid") && !targetDict.ContainsKey("patientordersuid")) MapProperty(sourceAsDict, targetDict, "patientordersuid");
            return (ExpandoObject)targetDict;
        }

        private static IEnumerable<ExpandoObject> FlattenAndTransformDispenseBatchDetail(ExpandoObject poItemRow, ICollection<string> cols)
        {
            var poItemDict = (IDictionary<string, object?>)poItemRow;
            if (poItemDict.TryGetValue("dispensebatchdetail", out object? val) && val is IEnumerable<object> details)
            {
                foreach (object? detail in details)
                {
                    if (detail == null) continue;
                    var targetDetail = TransformObject((ExpandoObject)detail, cols);
                    var targetDict = (IDictionary<string, object?>)targetDetail;
                    targetDict["patientorderitemsuid"] = poItemDict["_id"];
                    yield return targetDetail;
                }
            }
        }

        private static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key)
        {
            if (source.TryGetValue(key, out object? value))
            {
                if (value == null) { target[key] = DBNull.Value; return; }
                if (value is DateTime utc)
                {
                    try { target[key] = TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time")); }
                    catch (TimeZoneNotFoundException) { target[key] = utc.AddHours(7); }
                }
                else if (value is ObjectId oid) { target[key] = oid.ToString(); }
                else if (value is IEnumerable && value is not string)
                {
                    if (key == "dispensebatchdetail") { target[key] = value; }
                    else { target[key] = JsonSerializer.Serialize(value); }
                }
                else { target[key] = value; }
            }
            else { target[key] = DBNull.Value; }
        }

        private static void BuildAndLinkPipeline(
            MongoDbSource<ExpandoObject> source,
            CustomDestination<ETLBoxError> logErrors,
            RowTransformation<ExpandoObject> transformPatientOrders,
            RowMultiplication<ExpandoObject, ExpandoObject> flattenAndNormalizeOrderItems,
            RowTransformation<ExpandoObject> transformOrderItemForSql,
            RowMultiplication<ExpandoObject, ExpandoObject> flattenDispenseBatchDetails,
            DbMerge<ExpandoObject> destPatientOrders,
            DbMerge<ExpandoObject> destPatientOrderItems,
            DbMerge<ExpandoObject> destDispenseBatchDetails)
        {
            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastNormalizedItems = new Multicast<ExpandoObject>();

            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);
            transformPatientOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            multicastOrders.LinkTo(flattenAndNormalizeOrderItems);
            flattenAndNormalizeOrderItems.LinkErrorTo(logErrors);

            flattenAndNormalizeOrderItems.LinkTo(multicastNormalizedItems);

            multicastNormalizedItems.LinkTo(transformOrderItemForSql);
            transformOrderItemForSql.LinkTo(destPatientOrderItems);
            transformOrderItemForSql.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            multicastNormalizedItems.LinkTo(flattenDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkErrorTo(logErrors);
            destDispenseBatchDetails.LinkErrorTo(logErrors);
        }
        #endregion
    }
}
