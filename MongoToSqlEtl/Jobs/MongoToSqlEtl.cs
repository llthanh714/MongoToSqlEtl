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

        // ROBUSTNESS FIX: Use ConcurrentBag for thread-safe collection of failed IDs.
        // This avoids potential race conditions when the error destination is called from multiple threads.
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

                // ROBUSTNESS FIX: Ensure that the error destination is always created, even if no errors are expected.
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

        protected virtual CustomDestination<ETLBoxError> CreateErrorLoggingDestination(PerformContext? context)
        {
            return new CustomDestination<ETLBoxError>
            {
                WriteAction = (error, rowIndex) =>
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

                        // Hàm nội tuyến để xử lý ghi log cho một record
                        void LogSingleRecordFailure(IDictionary<string, object?> recordDict, string reason)
                        {
                            if (recordDict != null && recordDict.TryGetValue("_id", out var idValue) && idValue != null)
                            {
                                string? recordId = idValue.ToString();
                                context?.WriteLine($"Data Row Error. ID: {recordId}. Reason: {reason}");

                                if (!string.IsNullOrEmpty(recordId))
                                {
                                    Log.Warning("Data Row Error. ID: {RecordId}. Reason: {Reason}", recordId, reason);
                                    FailedRecordManager.LogFailedRecord(recordId, reason);
                                    CurrentRunFailedIds.Add(recordId);
                                }
                                else
                                {
                                    Log.Warning("Data Row Error. No valid ID found in the record. Reason: {Reason}", reason);
                                }
                            }
                            else
                            {
                                Log.Warning("Could not find a valid ID in the error record to log. Data: {@ErrorRecord}", recordDict);
                            }
                        }

                        using var jsonDoc = JsonDocument.Parse(json);
                        if (jsonDoc.RootElement.ValueKind == JsonValueKind.Array)
                        {
                            // === LOGIC MỚI: XỬ LÝ LỖI THEO LÔ THÔNG MINH HƠN ===
                            Log.Warning(exception, "A data batch failed to write to SQL Server. Initiating re-validation to find specific culprits...");
                            var records = JsonSerializer.Deserialize<List<ExpandoObject>>(json);
                            if (records == null || records.Count == 0) return;

                            // Chỉ lấy TableDefinition một lần cho cả lô (giả định lô thuộc 1 bảng)
                            // Đây là một giả định hợp lý cho hầu hết các luồng ETL
                            var firstRecord = records.First() as IDictionary<string, object?>;
                            string? tableName = firstRecord?.FirstOrDefault(kvp => kvp.Key.StartsWith("stg_")).Key;

                            // Nếu không xác định được bảng, ghi log chung cho tất cả
                            if (tableName == null)
                            {
                                Log.Error("Could not determine destination table for the failed batch. Logging all records in batch as failed.");
                                foreach (var record in records) LogSingleRecordFailure(record, exception.ToString());
                                return;
                            }

                            var destTableDef = TableDefinition.FromTableName(SqlConnectionManager, tableName);

                            // Tái xác thực từng record trong lô
                            foreach (var record in records)
                            {
                                var recordDict = (IDictionary<string, object?>)record;
                                bool isRecordValid = true;
                                string validationError = string.Empty;

                                foreach (var col in destTableDef.Columns)
                                {
                                    if (col.DataType == "DATETIME" && recordDict.TryGetValue(col.Name, out var value))
                                    {
                                        if (value != null && value is not DateTime && !DateTime.TryParse(value.ToString(), out _))
                                        {
                                            isRecordValid = false;
                                            validationError = $"Invalid value '{value}' for DateTime column '{col.Name}'.";
                                            break; // Dừng kiểm tra record này
                                        }
                                    }
                                    // Thêm các quy tắc xác thực khác ở đây nếu cần (e.g., for numbers)
                                }

                                if (!isRecordValid)
                                {
                                    // Chỉ log những record thực sự bị lỗi
                                    LogSingleRecordFailure(recordDict, validationError);
                                }
                            }
                        }
                        else if (jsonDoc.RootElement.ValueKind == JsonValueKind.Object)
                        {
                            // Xử lý lỗi cho một record đơn lẻ như cũ
                            var record = JsonSerializer.Deserialize<IDictionary<string, object?>>(json);
                            if (record != null) LogSingleRecordFailure(record, exception.ToString());
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
                }
            };
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
    }
}