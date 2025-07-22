using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Console;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Services;
using Serilog;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// Lớp triển khai cụ thể cho việc ETL collection 'patientorders'.
    /// </summary>
    public class PatientOrdersEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        protected override string SourceCollectionName => "patientorders";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestPatientOrdersTable = "stg_patientorders";
        private const string DestPatientOrderItemsTable = "stg_patientorderitems";
        private const string DestPatientDiagnosisuidsTable = "stg_patientdiagnosisuids";
        private const string DestDispenseBatchDetailTable = "stg_dispensebatchdetail";

        public new async Task RunAsync(PerformContext? context)
        {
            context?.WriteLine("Starting job execution...");
            await base.RunAsync(context);
            context?.WriteLine("Job execution completed.");
        }

        protected override EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds, PerformContext? context)
        {
            var patientordersDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrdersTable);
            var patientorderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrderItemsTable);
            var patientdiagnosisuidsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var dispensebatchdetailDef = TableDefinition.FromTableName(SqlConnectionManager, DestDispenseBatchDetailTable);

            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination(context);

            var itemFieldsToKeepAsObject = new HashSet<string> { "dispensebatchdetail" };

            // Create transformation components
            var transformPatientOrders = DataTransformer.CreateTransformComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var transformOrderItemForSql = DataTransformer.CreateTransformComponent([.. patientorderitemsDef.Columns.Select(c => c.Name)]);
            var transformDiagnosisUidsSql = DataTransformer.CreateTransformComponent([.. patientdiagnosisuidsDef.Columns.Select(c => c.Name)]);

            // FIX: Use dedicated flattening logic for different array types
            var flattenAndNormalizeOrderItems = CreateFlattenAndNormalizeObjectsComponent("patientorderitems", itemFieldsToKeepAsObject);
            var flattenAndNormalizeDiagnosisUids = CreateFlattenAndNormalizeObjectsComponent("patientdiagnosisuids", itemFieldsToKeepAsObject);
            var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation([.. dispensebatchdetailDef.Columns.Select(c => c.Name)]);

            // Create destination components
            //var destPatientOrders = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrdersTable);
            //var destPatientOrderItems = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrderItemsTable);
            //var destPatientDiagnosisUids = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            //var destDispenseBatchDetail = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestDispenseBatchDetailTable);

            var destPatientOrders = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestPatientOrdersTable };
            var destPatientOrderItems = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestPatientOrderItemsTable };
            var destPatientDiagnosisUids = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestPatientDiagnosisuidsTable };
            var destDispenseBatchDetail = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestDispenseBatchDetailTable };

            // Create multicast components
            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastNormalizedItems = new Multicast<ExpandoObject>();

            // Main flow from source
            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // Flow 1: patientorders
            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);
            transformPatientOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            // Flow 2: patientdiagnosisuids
            multicastOrders.LinkTo(flattenAndNormalizeDiagnosisUids);
            flattenAndNormalizeDiagnosisUids.LinkTo(transformDiagnosisUidsSql);
            transformDiagnosisUidsSql.LinkTo(destPatientDiagnosisUids);
            flattenAndNormalizeDiagnosisUids.LinkErrorTo(logErrors);
            transformDiagnosisUidsSql.LinkErrorTo(logErrors);
            destPatientDiagnosisUids.LinkErrorTo(logErrors);

            // Flow 3: patientorderitems (which then splits again)
            multicastOrders.LinkTo(flattenAndNormalizeOrderItems);
            flattenAndNormalizeOrderItems.LinkTo(multicastNormalizedItems);
            flattenAndNormalizeOrderItems.LinkErrorTo(logErrors);

            // Flow 3.1: patientorderitems main data
            multicastNormalizedItems.LinkTo(transformOrderItemForSql);
            transformOrderItemForSql.LinkTo(destPatientOrderItems);
            transformOrderItemForSql.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            // Flow 3.2: dispensebatchdetail nested within patientorderitems
            multicastNormalizedItems.LinkTo(flattenDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetail);
            flattenDispenseBatchDetails.LinkErrorTo(logErrors);
            destDispenseBatchDetail.LinkErrorTo(logErrors);

            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destPatientDiagnosisUids, destDispenseBatchDetail],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: "sp_MergePatientOrdersData"
            );
        }

        #region Transformation Logic Specific to PatientOrders

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndNormalizeObjectsComponent(string arrayFieldName, HashSet<string> keepAsObjectFields)
        {
            var localArrayFieldName = arrayFieldName;
            var localKeepAsObjectFields = keepAsObjectFields;
            var localForeignKeyName = "patientordersuid";

            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => FlattenAndNormalizeObjects(
                parentRow,
                localArrayFieldName,
                localKeepAsObjectFields,
                localForeignKeyName));
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateDispenseBatchDetailsTransformation(HashSet<string> cols)
        {
            var localCols = cols;
            return new RowMultiplication<ExpandoObject, ExpandoObject>(itemRow => FlattenAndTransformDispenseBatchDetail(itemRow, localCols, "patientorderitemsuid"));
        }

        private static List<ExpandoObject> FlattenAndNormalizeObjects(
            ExpandoObject parentRow,
            string arrayFieldName,
            HashSet<string> keepAsObjectFields,
            string foreignKeyName)
        {
            var results = new List<ExpandoObject>();
            var parentAsDict = (IDictionary<string, object?>)parentRow;

            string parentIdAsString = parentAsDict.TryGetValue("_id", out var parentIdValue)
                ? parentIdValue?.ToString() ?? string.Empty
                : string.Empty;

            if (parentAsDict.TryGetValue(arrayFieldName, out object? value) && value is IEnumerable<object> items)
            {
                foreach (object? sourceItem in items)
                {
                    if (sourceItem == null) continue;

                    var transformedItem = (sourceItem is ExpandoObject expando)
                        ? expando
                        : ConvertToExando(sourceItem);

                    if (transformedItem == null)
                    {
                        Log.Warning("Skipping null transformed item in {ArrayFieldName} for parent ID {ParentId}", arrayFieldName, parentIdAsString);
                        continue;
                    }

                    var newItem = DataTransformer.TransformObject(transformedItem, [], keepAsObjectFields);
                    var newItemDict = (IDictionary<string, object?>)newItem;

                    // FIX: Chỉ thêm khóa ngoại nếu nó chưa tồn tại
                    if (!newItemDict.ContainsKey(foreignKeyName))
                    {
                        newItemDict[foreignKeyName] = parentIdAsString;
                    }

                    results.Add(newItem);
                }
            }
            return results;
        }

        private static List<ExpandoObject> FlattenAndTransformDispenseBatchDetail(ExpandoObject poItemRow, ICollection<string> cols, string foreignKeyName)
        {
            var results = new List<ExpandoObject>();
            var poItemDict = (IDictionary<string, object?>)poItemRow;

            if (poItemDict.TryGetValue("dispensebatchdetail", out object? val) && val is IEnumerable<object> details)
            {
                foreach (object? detail in details)
                {
                    if (detail == null) continue;

                    var expandoDetail = (detail is ExpandoObject exp) ? exp : ConvertToExando(detail);

                    if (expandoDetail == null)
                    {
                        Log.Warning("Skipping null ExpandoObject in dispensebatchdetail for patientorderitemsuid");
                        continue;
                    }

                    var targetDetail = DataTransformer.TransformObject(expandoDetail, cols);
                    var targetDict = (IDictionary<string, object?>)targetDetail;

                    if (poItemDict.TryGetValue("_id", out var poItemId) && poItemId != null)
                    {
                        targetDict[foreignKeyName] = poItemId;
                    }

                    results.Add(targetDetail);
                }
            }
            return results;
        }

        private static ExpandoObject? ConvertToExando(object obj)
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
                // Điều này giúp chẩn đoán các vấn đề về dữ liệu không mong muốn.
                Log.Warning(ex, "Could not convert object to ExpandoObject. Object Type: {ObjectType}", obj?.GetType().FullName ?? "null");
                return null;
            }
        }

        #endregion
    }
}