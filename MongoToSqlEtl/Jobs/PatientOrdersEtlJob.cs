using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Console;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Services;
using System.Dynamic;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// Lớp triển khai cụ thể cho việc ETL collection 'patientorders'.
    /// </summary>
    public class PatientOrdersEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        protected override string SourceCollectionName => "patientorders";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestPatientOrdersTable = "patientorders";
        private const string DestPatientOrderItemsTable = "patientorderitems";
        private const string DestPatientDiagnosisuidsTable = "patientdiagnosisuids";
        private const string DestDispenseBatchDetailTable = "dispensebatchdetail";

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
            var destPatientOrders = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrdersTable);
            var destPatientOrderItems = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrderItemsTable);
            var destPatientDiagnosisUids = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var destDispenseBatchDetail = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestDispenseBatchDetailTable);

            // Create multicast components
            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastNormalizedItems = new Multicast<ExpandoObject>();

            // FIX: CRASH RISK: Link errors from ALL components to the error handler

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
                ErrorDestination: logErrors
            );
        }

        #region Transformation Logic Specific to PatientOrders

        // Renamed from CreateFlattenAndNormalizeComponent for clarity
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

        // Renamed from FlattenAndNormalizeItems for clarity
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

                    var newItem = DataTransformer.TransformObject(transformedItem, [], keepAsObjectFields);
                    var newItemDict = (IDictionary<string, object?>)newItem;

                    newItemDict[foreignKeyName] = parentIdAsString;
                    results.Add(newItem);
                }
            }
            return results;
        }

        // FIX: ARGUMENT EXCEPTION: Use indexer instead of Add() to prevent crash on duplicate properties.
        private static ExpandoObject ConvertToExando(object obj)
        {
            var expando = new ExpandoObject();
            var dict = (IDictionary<string, object?>)expando;

            if (obj is BsonDocument bsonDoc)
            {
                foreach (var element in bsonDoc.Elements)
                {
                    dict[element.Name] = BsonSerializer.Deserialize<object>(element.Value.ToBsonDocument());
                }
            }
            else
            {
                foreach (var property in obj.GetType().GetProperties())
                {
                    dict[property.Name] = property.GetValue(obj);
                }
            }
            return expando;
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

                    var targetDetail = DataTransformer.TransformObject(expandoDetail, cols);
                    var targetDict = (IDictionary<string, object?>)targetDetail;

                    targetDict[foreignKeyName] = poItemDict["_id"];
                    results.Add(targetDetail);
                }
            }
            return results;
        }

        #endregion
    }
}