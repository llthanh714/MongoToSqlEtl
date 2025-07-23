using ETLBox;
using ETLBox.ControlFlow;
using Microsoft.Data.SqlClient;
using Serilog;
using System.Data;

namespace MongoToSqlEtl.Managers
{
    public class EtlFailedRecordManager(IConnectionManager connectionManager, string sourceCollectionName)
    {
        public List<string> GetPendingFailedRecordIds()
        {
            var ids = new List<string>();
            try
            {
                var sql = "SELECT FailedRecordId FROM __ETLFailedRecords WHERE SourceCollectionName = @sourceCollectionName AND Status = 'Pending' AND RetryCount < 3";
                var parameters = new List<QueryParameter>
                {
                    new("sourceCollectionName", sourceCollectionName)
                };

                var task = new SqlTask(sql, parameters)
                {
                    ConnectionManager = connectionManager,
                    Actions = [id => ids.Add(id.ToString()!)]
                };
                task.ExecuteReader();
                Log.Information("[FailedManager] Found {Count} pending failed records to retry.", ids.Count);
            }
            catch (Exception ex) { Log.Warning(ex, "[FailedManager] Could not query __ETLFailedRecords table."); }
            return ids;
        }

        public void LogFailedRecord(string recordId, string errorMessage)
        {
            var sql = @"
                MERGE __ETLFailedRecords AS target
                USING (SELECT @sourceCollectionName AS SourceCollectionName, @failedRecordId AS FailedRecordId) AS source
                ON (target.SourceCollectionName = source.SourceCollectionName AND target.FailedRecordId = source.FailedRecordId)
                WHEN MATCHED THEN
                    UPDATE SET
                        Status = 'Pending',
                        ErrorMessage = @errorMessage,
                        LoggedAtUtc = GETUTCDATE(),
                        ResolvedAtUtc = NULL,
                        RetryCount = target.RetryCount + 1
                WHEN NOT MATCHED THEN
                    INSERT (SourceCollectionName, FailedRecordId, ErrorMessage, Status)
                    VALUES (@sourceCollectionName, @failedRecordId, @errorMessage, 'Pending');";

            var parameters = new List<QueryParameter>
            {
                new("sourceCollectionName", sourceCollectionName),
                new("failedRecordId", recordId),
                new("errorMessage", errorMessage)
            };

            try
            {
                SqlTask.ExecuteNonQuery(connectionManager, sql, parameters);
            }
            catch (Exception ex) { Log.Error(ex, "[FailedManager] Could not log failed record ID: {RecordId}", recordId); }
        }

        public void MarkRecordsAsResolved(List<string> recordIds)
        {
            if (recordIds.Count == 0) return;

            // FATAL ERROR FIX: Use raw ADO.NET to correctly handle Table-Valued Parameters (TVP),
            // bypassing potential issues with the ETLBox abstraction layer.
            var sql = @"
                UPDATE target
                SET Status = 'Resolved',
                    ResolvedAtUtc = GETUTCDATE()
                FROM __ETLFailedRecords AS target
                INNER JOIN @idList AS source ON target.FailedRecordId = source.Value
                WHERE target.SourceCollectionName = @sourceCollectionName
                  AND target.Status = 'Pending';";

            var idTable = new DataTable();
            idTable.Columns.Add("Value", typeof(string));
            foreach (var id in recordIds)
            {
                idTable.Rows.Add(id);
            }

            try
            {
                // FIX: Instead of cloning the connection manager, which caused access issues,
                // create a new SqlConnection directly from the connection string.
                // This is a more robust way to interact with ADO.NET directly.
                string? connectionString = connectionManager.ConnectionString.Value;
                using var conn = new SqlConnection(connectionString);
                conn.Open();
                using var cmd = new SqlCommand(sql, conn);

                cmd.Parameters.AddWithValue("@sourceCollectionName", sourceCollectionName);

                // Explicitly define the Table-Valued Parameter
                var tvpParam = cmd.Parameters.AddWithValue("@idList", idTable);
                tvpParam.SqlDbType = SqlDbType.Structured;
                tvpParam.TypeName = "dbo.udtt_StringList"; // Ensure this matches the type name in SQL Server

                int count = cmd.ExecuteNonQuery();
                Log.Information("[FailedManager] Marked {Count} previously pending records as 'Resolved'.", count);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[FailedManager] Could not mark records as resolved using TVP.");
            }
        }
    }
}