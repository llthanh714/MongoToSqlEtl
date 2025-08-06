using ETLBox;
using ETLBox.ControlFlow;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;

namespace MongoToSqlEtl.Managers
{
    public class EtlLogManager(IConnectionManager connectionManager, MongoClient mongoClient, string mongoDatabaseName, string sourceCollectionName)
    {

        // Please adjust this date to your project's actual start date for fallback scenarios.
        private static readonly DateTime FallbackStartDate = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateTime GetLastSuccessfulWatermark()
        {
            // Step 1: Try to get the watermark from the SQL Server log table
            try
            {
                var sql = @"
                    SELECT TOP 1 WatermarkEndTimeUtc FROM __ETLExecutionLog
                    WHERE SourceCollectionName = @sourceCollectionName AND Status = 'Succeeded'
                    ORDER BY ExecutionStartTimeUtc DESC";
                var parameters = new List<QueryParameter> { new("sourceCollectionName", sourceCollectionName) };

                var result = SqlTask.ExecuteScalar(connectionManager, sql, parameters);
                if (result != null && result != DBNull.Value)
                {
                    var lastRun = DateTime.SpecifyKind((DateTime)result, DateTimeKind.Utc);
                    Log.Information("[LogManager] Found last successful watermark in SQL: {LastRun}", lastRun);
                    return lastRun;
                }
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[LogManager] Could not query __ETLExecutionLog table.");
            }

            // Step 2: If no watermark is found, query MongoDB for the earliest 'modifiedat'
            Log.Information("[LogManager] No watermark found in SQL. Querying MongoDB for the earliest 'modifiedat' date.");
            try
            {
                var database = mongoClient.GetDatabase(mongoDatabaseName);
                var collection = database.GetCollection<BsonDocument>(sourceCollectionName);

                var sort = Builders<BsonDocument>.Sort.Ascending("modifiedat");
                var projection = Builders<BsonDocument>.Projection.Include("modifiedat");
                var earliestDoc = collection.Find(new BsonDocument()).Sort(sort).Project(projection).FirstOrDefault();

                if (earliestDoc != null && earliestDoc.Contains("modifiedat") && earliestDoc["modifiedat"].IsBsonDateTime)
                {
                    var earliestDate = earliestDoc["modifiedat"].ToUniversalTime();
                    Log.Information("[LogManager] Found earliest 'modifiedat' in MongoDB: {EarliestDate}", earliestDate);
                    return earliestDate;
                }
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[LogManager] Could not query MongoDB for the earliest date. This may happen if the collection is new or empty.");
            }

            // Step 3: If both SQL and MongoDB fail, use the hardcoded fallback start date
            Log.Information("[LogManager] No date found in MongoDB. Using hardcoded fallback: {FallbackDate}", FallbackStartDate);
            return FallbackStartDate;
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