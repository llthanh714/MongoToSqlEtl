using ETLBox;
using ETLBox.ControlFlow;
using Serilog;

namespace MongoToSqlEtl.Managers
{
    public class EtlFailedRecordManager(IConnectionManager connectionManager, string sourceCollectionName)
    {
        public List<string> GetPendingFailedRecordIds()
        {
            var ids = new List<string>();
            try
            {
                var sql = $"SELECT FailedRecordId FROM __ETLFailedRecords WHERE SourceCollectionName = '{sourceCollectionName}' AND Status = 'Pending'";
                var task = new SqlTask(sql)
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
            var sanitizedErrorMessage = errorMessage.Replace("'", "''");
            var sql = $@"
                MERGE __ETLFailedRecords AS target
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
                UPDATE __ETLFailedRecords
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
}
