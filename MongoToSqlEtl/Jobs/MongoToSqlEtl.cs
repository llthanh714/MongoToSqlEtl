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
    public abstract class EtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService)
    {
        protected readonly IConnectionManager SqlConnectionManager = sqlConnectionManager;
        protected readonly MongoClient MongoClient = mongoClient;
        protected readonly INotificationService NotificationService = notificationService;

        protected EtlLogManager LogManager = null!;
        protected EtlFailedRecordManager FailedRecordManager = null!;

        protected ConcurrentBag<string> CurrentRunFailedIds { get; private set; } = [];
        protected abstract List<string> StagingTables { get; }

        protected abstract string SourceCollectionName { get; }
        protected abstract string MongoDatabaseName { get; }

        protected abstract void SetJobSettings(JobSettings jobSettings);

        protected abstract EtlPipeline BuildPipeline(List<ExpandoObject> batchData, PerformContext? context);

        [DisableConcurrentExecution(timeoutInSeconds: 15 * 60)]
        public async Task RunAsync(PerformContext? context, JobSettings jobSettings)
        {
            int logId = 0;
            try
            {
                SetJobSettings(jobSettings);

                LogManager = new EtlLogManager(SqlConnectionManager, SourceCollectionName);
                FailedRecordManager = new EtlFailedRecordManager(SqlConnectionManager, SourceCollectionName);

                long recordsToSkip = 0;
                if (jobSettings.Backfill.Enabled)
                {
                    recordsToSkip = LogManager.GetTotalBackfillRecordsProcessed();
                }

                var (batchData, newWatermark) = await FetchNextBatchAsync(recordsToSkip, jobSettings.MaxRecordsPerJob, jobSettings.Backfill.Enabled);

                if (batchData.Count == 0)
                {
                    var message = jobSettings.Backfill.Enabled
                        ? "Backfill completed. No more records to process."
                        : "No new data to process.";
                    context?.WriteLine(message);
                    Log.Information("[{JobName}] {Message}", SourceCollectionName, message);
                    return;
                }

                var previousWatermark = LogManager.GetLastSuccessfulWatermark();
                logId = LogManager.StartNewLogEntry(previousWatermark, newWatermark);

                await TruncateStagingTablesAsync(context);

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
                long failedCount = CurrentRunFailedIds.Count;

                context?.WriteLine($"Summary --> Source: {totalSourceCount}, Successful (Total Processed): {successCount}, Failed: {failedCount}");
                LogManager.UpdateLogEntryOnSuccess(logId, totalSourceCount, successCount, failedCount);
            }
            catch (Exception ex)
            {
                context?.WriteLine($"Job '{SourceCollectionName}' has failed with a critical error.");
                context?.WriteLine(ex.ToString());
                context?.ResetTextColor();
                if (logId > 0 && LogManager != null)
                {
                    LogManager.UpdateLogEntryOnFailure(logId, ex.ToString());
                    await NotificationService.SendFatalErrorAsync(SourceCollectionName, ex);
                }
                throw;
            }
        }

        protected virtual async Task<(List<ExpandoObject> Data, EtlWatermark NewWatermark)> FetchNextBatchAsync(long recordsToSkip, int batchSize, bool isBackfill)
        {
            var collection = MongoClient.GetDatabase(MongoDatabaseName).GetCollection<BsonDocument>(SourceCollectionName);
            var documentMap = new Dictionary<string, BsonDocument>();

            var pendingFailedIds = FailedRecordManager.GetPendingFailedRecordIds();
            if (pendingFailedIds.Count != 0)
            {
                Log.Information("[{JobName}] Found {Count} pending records to retry. Fetching their full data.", SourceCollectionName, pendingFailedIds.Count);
                var retryFilter = Builders<BsonDocument>.Filter.In("_id", pendingFailedIds.Select(id => new ObjectId(id)));
                var failedDocs = await collection.Find(retryFilter).ToListAsync();

                foreach (var doc in failedDocs)
                {
                    var idString = doc["_id"].AsObjectId.ToString();
                    documentMap.TryAdd(idString, doc);
                }
            }

            var remainingBatchSize = batchSize - documentMap.Count;
            if (remainingBatchSize > 0)
            {
                var filterBuilder = Builders<BsonDocument>.Filter;
                FilterDefinition<BsonDocument> filter;
                SortDefinition<BsonDocument> sort;

                if (isBackfill)
                {
                    Log.Information("[{JobName}] Running in Backfill mode. Skipping: {Skip}, Taking: {Take}", SourceCollectionName, recordsToSkip, remainingBatchSize);
                    filter = filterBuilder.Empty;
                    sort = Builders<BsonDocument>.Sort.Ascending("_id");
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
                var newDocuments = await findQuery.Limit(remainingBatchSize).ToListAsync();

                foreach (var doc in newDocuments)
                {
                    var idString = doc["_id"].IsObjectId ? doc["_id"].AsObjectId.ToString() : doc["_id"].AsString;
                    documentMap.TryAdd(idString, doc);
                }
            }

            if (documentMap.Count == 0)
            {
                return ([], new EtlWatermark(DateTime.MinValue, null));
            }

            var documents = documentMap.Values.ToList();

            documents.Sort((a, b) =>
            {
                var valA = a.Contains("modifiedat") ? a["modifiedat"] : a["_id"];
                var valB = b.Contains("modifiedat") ? b["modifiedat"] : b["_id"];
                if (valA.BsonType == valB.BsonType) return valA.CompareTo(valB);
                return valA.BsonType == BsonType.DateTime ? 1 : -1;
            });

            var lastDoc = documents.Last();
            var lastIdValue = lastDoc["_id"].IsObjectId ? lastDoc["_id"].AsObjectId.ToString() : lastDoc["_id"].AsString;
            var newWatermark = new EtlWatermark(lastDoc.Contains("modifiedat") ? lastDoc["modifiedat"].ToUniversalTime() : DateTime.UtcNow, lastIdValue);

            var expandoData = documents.Select(ConvertToExando).Where(e => e != null).Cast<ExpandoObject>().ToList();

            if (pendingFailedIds.Count != 0)
            {
                FailedRecordManager.MarkRecordsAsResolved(pendingFailedIds);
            }

            Log.Information("[{JobName}] Fetched {Count} records ({RetryCount} retries, {NewCount} new). New watermark: {Watermark}", SourceCollectionName, expandoData.Count, pendingFailedIds.Count, documents.Count - pendingFailedIds.Count, newWatermark);
            return (expandoData, newWatermark);
        }

        protected static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenComponent(string arrayFieldName, string foreignKeyName, string parentIdFieldName = "id")
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => Flatten(parentRow, arrayFieldName, foreignKeyName, parentIdFieldName));
        }

        private static IEnumerable<ExpandoObject> Flatten(ExpandoObject parentRow, string arrayFieldName, string foreignKeyName, string parentIdFieldName)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            if (!parentAsDict.TryGetValue(parentIdFieldName, out var parentIdValue)) yield break;

            var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;
            if (!parentAsDict.TryGetValue(arrayFieldName, out object? value) || value == null) yield break;

            var itemsToProcess = new List<object>();
            if (value is JsonElement jsonElement && jsonElement.ValueKind == JsonValueKind.Array)
            {
                itemsToProcess.AddRange(jsonElement.EnumerateArray().Select(e => (object)e));
            }
            else if (value is IEnumerable<object> list && value is not string)
            {
                itemsToProcess.AddRange(list);
            }
            else
            {
                itemsToProcess.Add(value);
            }

            foreach (var item in itemsToProcess)
            {
                if (item == null) continue;
                var itemAsExpando = ConvertToExando(item);
                if (itemAsExpando == null) continue;

                var itemCopy = new ExpandoObject();
                var itemDict = (IDictionary<string, object?>)itemCopy;
                foreach (var kvp in (IDictionary<string, object?>)itemAsExpando) itemDict[kvp.Key] = kvp.Value;

                itemDict[foreignKeyName] = parentIdAsString;
                yield return itemCopy;
            }
        }

        protected static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndTransformComponent(string arrayFieldName, string foreignKeyName, ICollection<string> targetColumns, string parentIdFieldName = "id", HashSet<string>? keepAsObjectFields = null)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => FlattenAndTransform(parentRow, arrayFieldName, foreignKeyName, targetColumns, parentIdFieldName, keepAsObjectFields));
        }

        private static IEnumerable<ExpandoObject> FlattenAndTransform(ExpandoObject parentRow, string arrayFieldName, string foreignKeyName, ICollection<string> targetColumns, string parentIdFieldName, HashSet<string>? keepAsObjectFields)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            if (!parentAsDict.TryGetValue(parentIdFieldName, out var parentIdValue)) yield break;

            var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;
            if (!parentAsDict.TryGetValue(arrayFieldName, out object? value) || value == null) yield break;

            var itemsToProcess = new List<object>();
            if (value is JsonElement jsonElement && jsonElement.ValueKind == JsonValueKind.Array)
            {
                itemsToProcess.AddRange(jsonElement.EnumerateArray().Select(e => (object)e));
            }
            else if (value is IEnumerable<object> list && value is not string)
            {
                itemsToProcess.AddRange(list);
            }
            else
            {
                itemsToProcess.Add(value);
            }

            foreach (var item in itemsToProcess)
            {
                if (item == null) continue;
                var itemAsExpando = ConvertToExando(item);
                if (itemAsExpando == null) continue;

                var itemAsDict = (IDictionary<string, object?>)itemAsExpando;
                itemAsDict[foreignKeyName] = parentIdAsString;

                var transformedItem = DataTransformer.TransformObject(itemAsExpando, targetColumns, keepAsObjectFields);
                yield return transformedItem;
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
                    var rawJson = jsonElem.GetRawText();
                    if (string.IsNullOrWhiteSpace(rawJson) || !rawJson.Trim().StartsWith('{')) return null;
                    return JsonSerializer.Deserialize<ExpandoObject>(rawJson);
                }

                var generalJson = JsonSerializer.Serialize(obj);
                return JsonSerializer.Deserialize<ExpandoObject>(generalJson);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "Could not convert object to ExpandoObject. Object Type: {ObjectType}, Object Data: {@Object}", obj?.GetType().FullName ?? "null", obj);
                return null;
            }
        }

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
                        if (recordDict != null && recordDict.TryGetValue("id", out var idValue) && idValue != null)
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
            foreach (var tableName in tablesToTruncate) tableData.Rows.Add(tableName);

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
    }
}