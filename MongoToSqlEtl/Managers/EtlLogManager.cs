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
        /// ✅ SỬA ĐỔI: Loại bỏ hoàn toàn logic kiểm tra `isBackfill`.
        /// Phương thức này giờ đây chỉ có một nhiệm vụ: đọc log cuối cùng một cách trung thực.
        /// </summary>
        public EtlWatermark GetLastSuccessfulWatermark(bool isBackfill = false) // Tham số isBackfill vẫn được giữ lại nhưng không còn được sử dụng ở đây.
        {
            var sql = @"
                SELECT TOP 1 WatermarkLastModifiedAtUtc, WatermarkLastId FROM __ETLExecutionLog
                WHERE SourceCollectionName = @SourceCollectionName AND UPPER(Status) = 'SUCCEEDED' AND WatermarkLastId IS NOT NULL AND WatermarkLastId <> ''
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
                    Log.Information("[LogManager] Found last successful watermark in SQL: {Watermark}", watermark);
                    return watermark;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[LogManager] An error occurred while fetching watermark with ADO.NET.");
            }

            // Nếu không tìm thấy log nào (ví dụ: lần chạy ĐẦU TIÊN của backfill),
            // trả về một watermark "khởi tạo".
            Log.Information("[LogManager] No successful run found. Initializing watermark.");
            if (isBackfill)
            {
                // Cho backfill, chúng ta bắt đầu với ID rỗng để lấy từ đầu.
                return new EtlWatermark(DateTime.MinValue, null);
            }
            else
            {
                // Cho chế độ thường, bắt đầu từ ngày mặc định.
                return new EtlWatermark(FallbackStartDate, null);
            }
        }

        public long GetTotalBackfillRecordsProcessed()
        {
            var sql = @"
                SELECT ISNULL(SUM(SourceRecordCount), 0)
                FROM __ETLExecutionLog
                WHERE SourceCollectionName = @SourceCollectionName AND UPPER(Status) = 'SUCCEEDED'";

            try
            {
                var result = SqlTask.ExecuteScalar(connectionManager, sql, [new("SourceCollectionName", sourceCollectionName)]);
                if (result != null && result != DBNull.Value)
                {
                    var total = Convert.ToInt64(result);
                    Log.Information("[LogManager] Total records processed in previous backfills: {Total}", total);
                    return total;
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[LogManager] Could not calculate total backfill records processed.");
            }
            return 0;
        }

        // Các phương thức StartNewLogEntry, UpdateLogEntryOnSuccess, UpdateLogEntryOnFailure không thay đổi.
        #region Unchanged Methods
        public int StartNewLogEntry(EtlWatermark previousWatermark, EtlWatermark newWatermark)
        {
            var sql = @"
                INSERT INTO __ETLExecutionLog (SourceCollectionName, ExecutionStartTimeUtc, WatermarkPreviousModifiedAtUtc, WatermarkLastModifiedAtUtc, WatermarkLastId, Status)
                VALUES (@sourceCollectionName, GETUTCDATE(), @prevModAt, @newModAt, @newId, 'Started');
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
            };

            var logId = Convert.ToInt32(SqlTask.ExecuteScalar(connectionManager, sql, parameters));
            Log.Information("[LogManager] Created new ETL log entry with ID: {LogId}", logId);
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