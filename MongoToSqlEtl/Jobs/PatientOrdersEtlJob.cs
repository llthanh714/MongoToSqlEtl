using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Console;
using Hangfire.Server;
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

            var transformPatientOrders = DataTransformer.CreateTransformComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var flattenAndNormalizeOrderItems = CreateFlattenAndNormalizeOrderItems(itemFieldsToKeepAsObject);
            var transformOrderItemForSql = DataTransformer.CreateTransformComponent([.. patientorderitemsDef.Columns.Select(c => c.Name)]);
            var flattenAndNormalizeDiagnosisUids = CreateFlattenAndNormalizeDiagnosisUids(itemFieldsToKeepAsObject);
            var transformDiagnosisUidsSql = DataTransformer.CreateTransformComponent([.. patientdiagnosisuidsDef.Columns.Select(c => c.Name)]);
            var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation([.. dispensebatchdetailDef.Columns.Select(c => c.Name)]);

            var destPatientOrders = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrdersTable);
            var destPatientOrderItems = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrderItemsTable);
            var destPatientDiagnosisUids = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var destDispenseBatchDetail = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestDispenseBatchDetailTable);

            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastNormalizedItems = new Multicast<ExpandoObject>();


            // Tách luồng cho dữ liệu 'patientorders' và các mục liên quan
            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // Luồng 1: xử lý dữ liệu 'patientorders'
            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);
            transformPatientOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            // Luồng 2: xử lý dữ liệu 'patientdiagnosisuids'
            multicastOrders.LinkTo(flattenAndNormalizeDiagnosisUids);
            flattenAndNormalizeDiagnosisUids.LinkTo(transformDiagnosisUidsSql);
            transformDiagnosisUidsSql.LinkTo(destPatientDiagnosisUids);
            destPatientDiagnosisUids.LinkErrorTo(logErrors);

            // Luồng 3: Tách 'patientorderitems' ra thành các luồng con
            multicastOrders.LinkTo(flattenAndNormalizeOrderItems);
            flattenAndNormalizeOrderItems.LinkErrorTo(logErrors);

            // Luồng 3.1: Xử lý các mục 'patientorderitems'
            flattenAndNormalizeOrderItems.LinkTo(multicastNormalizedItems);
            multicastNormalizedItems.LinkTo(transformOrderItemForSql);
            transformOrderItemForSql.LinkTo(destPatientOrderItems);
            transformOrderItemForSql.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            // Luồng 3.2: Xử lý các chi tiết 'dispensebatchdetail'
            multicastNormalizedItems.LinkTo(flattenDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetail);
            flattenDispenseBatchDetails.LinkErrorTo(logErrors);
            destDispenseBatchDetail.LinkErrorTo(logErrors);

            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destDispenseBatchDetail],
                ErrorDestination: logErrors
            );
        }

        #region Transformation Logic Specific to PatientOrders

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndNormalizeOrderItems(HashSet<string> keepAsObjectFields)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => FlattenAndNormalizeItems(
                parentRow,
                keepAsObjectFields,
                "patientordersuid"));
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndNormalizeDiagnosisUids(HashSet<string> keepAsObjectFields)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => FlattenAndNormalizeItems(
                parentRow,
                keepAsObjectFields,
                "patientordersuid"));
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateDispenseBatchDetailsTransformation(HashSet<string> cols)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(itemRow => FlattenAndTransformDispenseBatchDetail(itemRow, cols, "patientorderitemsuid"));
        }

        private static List<ExpandoObject> FlattenAndNormalizeItems(
            ExpandoObject parentRow,
            HashSet<string> keepAsObjectFields,
            string foreignKeyName)
        {
            // This method now returns a List to avoid iterator block issues with locking.
            var results = new List<ExpandoObject>();
            var parentAsDict = (IDictionary<string, object?>)parentRow;

            string parentIdAsString = parentAsDict.TryGetValue("_id", out var parentIdValue)
                ? parentIdValue?.ToString() ?? string.Empty
                : string.Empty;

            if (parentAsDict.TryGetValue("patientorderitems", out object? value) && value is IEnumerable<object> orderItems)
            {
                foreach (object? sourceItem in orderItems)
                {
                    if (sourceItem == null) continue;

                    // The TransformObject method is now thread-safe with an internal lock.
                    var newItem = DataTransformer.TransformObject((ExpandoObject)sourceItem, [], keepAsObjectFields);
                    var newItemDict = (IDictionary<string, object?>)newItem;

                    // Reliably overwrite the foreign key with the provided name
                    newItemDict[foreignKeyName] = parentIdAsString;
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

                    // The TransformObject method is now thread-safe with an internal lock.
                    var targetDetail = DataTransformer.TransformObject((ExpandoObject)detail, cols);
                    var targetDict = (IDictionary<string, object?>)targetDetail;

                    // Reliably overwrite the foreign key with the provided name
                    targetDict[foreignKeyName] = poItemDict["_id"];
                    results.Add(targetDetail);
                }
            }
            return results;
        }

        #endregion
    }
}
