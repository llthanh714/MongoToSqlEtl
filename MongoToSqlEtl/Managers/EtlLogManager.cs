using ETLBox;
using ETLBox.ControlFlow;
using Microsoft.Data.SqlClient;
using Serilog;
using System.Data.SqlTypes;

namespace MongoToSqlEtl.Managers
{
    public record EtlWatermark(DateTime LastModifiedAt, string? LastId);

    public class EtlLogManager(IConnectionManager connectionManager, string sourceCollectionName)
    {
        private static readonly DateTime FallbackStartDate = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Lấy watermark TOÀN CỤC. Hàm này tìm mốc thời gian thành công gần đây nhất
        /// bất kể nó được tạo bởi job 'Live' hay 'Backfill'.
        /// Điều này tối quan trọng để job Live biết chính xác nơi để bắt đầu.
        /// </summary>
        public EtlWatermark GetLastSuccessfulWatermark()
        {
            var sql = @"
                SELECT TOP 1 WatermarkLastModifiedAtUtc, WatermarkLastId FROM __ETLExecutionLog
                WHERE SourceCollectionName = @SourceCollectionName 
                  AND UPPER(Status) = 'SUCCEEDED' 
                  AND WatermarkLastId IS NOT NULL AND WatermarkLastId <> ''
                ORDER BY WatermarkLastModifiedAtUtc DESC, Id DESC";

            try
            {
                using var conn = new SqlConnection(connectionManager.ConnectionString.Value);
                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@SourceCollectionName", sourceCollectionName);
                conn.Open();
                using var reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    var lastRunTime = reader.GetDateTime(0);
                    var lastId = reader.GetString(1);
                    var watermark = new EtlWatermark(DateTime.SpecifyKind(lastRunTime, DateTimeKind.Utc), lastId);
                    Log.Information("[LogManager-Global] Found last successful GLOBAL watermark: {Watermark}", watermark);
                    return watermark;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[LogManager-Global] An error occurred while fetching global watermark.");
            }

            Log.Information("[LogManager-Global] No successful run found of any kind. Initializing watermark.");
            return new EtlWatermark(FallbackStartDate, null);
        }

        /// <summary>
        /// Lấy watermark CHỈ dành cho job BACKFILL.
        /// Hàm này hoạt động độc lập và chỉ phục vụ cho logic phân trang của backfill.
        /// </summary>
        public EtlWatermark? GetLastBackfillWatermark()
        {
            var sql = @"
                SELECT TOP 1 WatermarkLastModifiedAtUtc, WatermarkLastId 
                FROM __ETLExecutionLog
                WHERE SourceCollectionName = @SourceCollectionName 
                  AND UPPER(Status) = 'SUCCEEDED'
                  AND JobMode = 'Backfill'
                  AND WatermarkLastId IS NOT NULL AND WatermarkLastId <> ''
                ORDER BY Id DESC"; // Sắp xếp theo ID giảm dần để lấy lần chạy cuối cùng

            try
            {
                using var conn = new SqlConnection(connectionManager.ConnectionString.Value);
                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@SourceCollectionName", sourceCollectionName);
                conn.Open();
                using var reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    var lastRunTime = reader.GetDateTime(0);
                    var lastId = reader.GetString(1);
                    var watermark = new EtlWatermark(DateTime.SpecifyKind(lastRunTime, DateTimeKind.Utc), lastId);
                    Log.Information("[LogManager-Backfill] Found last successful backfill watermark: {Watermark}", watermark);
                    return watermark;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[LogManager-Backfill] An error occurred while fetching backfill watermark.");
            }

            Log.Information("[LogManager-Backfill] No successful backfill run found. This must be the first run.");
            return null;
        }

        #region Unchanged Methods
        public int StartNewLogEntry(EtlWatermark previousWatermark, EtlWatermark newWatermark, bool isBackfill)
        {
            var sql = @"
                INSERT INTO __ETLExecutionLog (SourceCollectionName, ExecutionStartTimeUtc, WatermarkPreviousModifiedAtUtc, WatermarkLastModifiedAtUtc, WatermarkLastId, Status, JobMode)
                VALUES (@sourceCollectionName, GETUTCDATE(), @prevModAt, @newModAt, @newId, 'Started', @jobMode);
                SELECT SCOPE_IDENTITY();";

            var safePreviousModifiedAt = previousWatermark.LastModifiedAt == DateTime.MinValue
                                       ? SqlDateTime.MinValue.Value
                                       : previousWatermark.LastModifiedAt;

            var parameters = new List<QueryParameter>
            {
                new("sourceCollectionName", sourceCollectionName),
                new("prevModAt", safePreviousModifiedAt),
                new("newModAt", newWatermark.LastModifiedAt),
                new("newId", (object?)newWatermark.LastId ?? DBNull.Value),
                new("jobMode", isBackfill ? "Backfill" : "Live")
            };

            var logId = Convert.ToInt32(SqlTask.ExecuteScalar(connectionManager, sql, parameters));
            Log.Information("[LogManager] Created new ETL log entry with ID: {LogId} for a '{JobMode}' job.", logId, isBackfill ? "Backfill" : "Live");
            return logId;
        }

        public void UpdateLogEntryOnSuccess(int logId, long sourceCount, long successCount, long failedCount)
        {
            var sql = @"UPDATE __ETLExecutionLog SET ExecutionEndTimeUtc = GETUTCDATE(), Status = 'Succeeded',
                SourceRecordCount = @sourceCount, SuccessRecordCount = @successCount, FailedRecordCount = @failedCount, ErrorMessage = NULL
                WHERE Id = @logId;";
            var parameters = new List<QueryParameter>
            {
                new("sourceCount", sourceCount),
                new("successCount", successCount),
                new("failedCount", failedCount),
                new("logId", logId)
            };
            SqlTask.ExecuteNonQuery(connectionManager, sql, parameters);
            Log.Information("[LogManager] Updated log entry ID: {LogId} to 'Succeeded'.", logId);
        }

        public void UpdateLogEntryOnFailure(int logId, string errorMessage)
        {
            var sql = @"UPDATE __ETLExecutionLog SET ExecutionEndTimeUtc = GETUTCDATE(), Status = 'Failed', ErrorMessage = @errorMessage
                WHERE Id = @logId;";
            var parameters = new List<QueryParameter>
            {
                new("errorMessage", errorMessage),
                new("logId", logId)
            };
            SqlTask.ExecuteNonQuery(connectionManager, sql, parameters);
            Log.Error("[LogManager] Updated log entry ID: {LogId} to 'Failed'.", logId);
        }
        #endregion
    }
}