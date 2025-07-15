using ETLBox;
using ETLBox.ControlFlow;
using Serilog;

namespace MongoToSqlEtl.Managers
{
    public class EtlLogManager(IConnectionManager connectionManager, string sourceCollectionName)
    {
        // Please adjust this date to your project's actual start date.
        private static readonly DateTime DefaultStartDate = new(2025, 7, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateTime GetLastSuccessfulWatermark()
        {
            try
            {
                // SECURITY FIX: Use parameterized query to prevent SQL injection.
                var sql = @"
                    SELECT TOP 1 WatermarkEndTimeUtc FROM __ETLExecutionLog
                    WHERE SourceCollectionName = @sourceCollectionName AND Status = 'Succeeded'
                    ORDER BY ExecutionStartTimeUtc DESC";
                var parameters = new List<QueryParameter>
                {
                    new("sourceCollectionName", sourceCollectionName)
                };

                var result = SqlTask.ExecuteScalar(connectionManager, sql, parameters);
                if (result != null && result != DBNull.Value)
                {
                    var lastRun = DateTime.SpecifyKind((DateTime)result, DateTimeKind.Utc);
                    Log.Information("[LogManager] Found last successful watermark: {LastRun}", lastRun);
                    return lastRun;
                }
            }
            catch (Exception ex) { Log.Warning(ex, "[LogManager] Could not query __ETLExecutionLog table."); }
            Log.Information("[LogManager] No watermark found, using default: {DefaultDate}", DefaultStartDate);
            return DefaultStartDate;
        }

        public int StartNewLogEntry(DateTime watermarkStart, DateTime watermarkEnd)
        {
            // SECURITY FIX: Use parameterized query to prevent SQL injection.
            var sql = @"
                INSERT INTO __ETLExecutionLog (SourceCollectionName, ExecutionStartTimeUtc, WatermarkStartTimeUtc, WatermarkEndTimeUtc, Status)
                VALUES (@sourceCollectionName, GETUTCDATE(), @watermarkStart, @watermarkEnd, 'Started');
                SELECT SCOPE_IDENTITY();";
            var parameters = new List<QueryParameter>
            {
                new("sourceCollectionName", sourceCollectionName),
                new("watermarkStart", watermarkStart),
                new("watermarkEnd", watermarkEnd)
            };
            var logId = Convert.ToInt32(SqlTask.ExecuteScalar(connectionManager, sql, parameters));
            Log.Information("[LogManager] Created new ETL log entry with ID: {LogId}", logId);
            return logId;
        }

        public void UpdateLogEntryOnSuccess(int logId, long sourceCount, long successCount, long failedCount)
        {
            // SECURITY FIX: Use parameterized query to prevent SQL injection.
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
            // SECURITY FIX: Use parameterized query to prevent SQL injection.
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
    }
}
