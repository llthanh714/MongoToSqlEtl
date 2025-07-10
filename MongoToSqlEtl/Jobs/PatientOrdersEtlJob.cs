using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using System.Dynamic;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// Lớp triển khai cụ thể cho việc ETL collection 'patientorders'.
    /// </summary>
    public class PatientOrdersEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient) : EtlJob(sqlConnectionManager, mongoClient)
    {
        // Định nghĩa các hằng số cho tên collection và bảng
        protected override string SourceCollectionName => "patientorders";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestPatientOrdersTable = "patientorders";
        private const string DestPatientOrderItemsTable = "patientorderitems";
        private const string DestDispenseBatchDetailTable = "dispensebatchdetail";

        /// <summary>
        /// Triển khai việc xây dựng và liên kết pipeline cho 'patientorders'.
        /// </summary>
        protected override EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds)
        {
            // Lấy schema bảng đích
            var patientordersDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrdersTable);
            var patientorderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrderItemsTable);
            var dispensebatchdetailDef = TableDefinition.FromTableName(SqlConnectionManager, DestDispenseBatchDetailTable);

            // Tạo các component của pipeline
            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination();

            // Cấu hình cụ thể cho job này
            var itemFieldsToKeepAsObject = new HashSet<string> { "dispensebatchdetail" };

            // Tạo các component transformation
            var transformPatientOrders = DataTransformer.CreateTransformComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var flattenAndNormalizeOrderItems = CreateFlattenAndNormalizeOrderItems(itemFieldsToKeepAsObject);
            var transformOrderItemForSql = DataTransformer.CreateTransformComponent([.. patientorderitemsDef.Columns.Select(c => c.Name)]);
            var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation([.. dispensebatchdetailDef.Columns.Select(c => c.Name)]);

            // Tạo các component destination
            var destPatientOrders = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrdersTable);
            var destPatientOrderItems = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestPatientOrderItemsTable);
            var destDispenseBatchDetail = DataTransformer.CreateDbMergeDestination(SqlConnectionManager, DestDispenseBatchDetailTable);

            // Liên kết pipeline
            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastNormalizedItems = new Multicast<ExpandoObject>();

            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);
            transformPatientOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            multicastOrders.LinkTo(flattenAndNormalizeOrderItems);
            flattenAndNormalizeOrderItems.LinkErrorTo(logErrors);

            flattenAndNormalizeOrderItems.LinkTo(multicastNormalizedItems);

            multicastNormalizedItems.LinkTo(transformOrderItemForSql);
            transformOrderItemForSql.LinkTo(destPatientOrderItems);
            transformOrderItemForSql.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            multicastNormalizedItems.LinkTo(flattenDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetail);
            flattenDispenseBatchDetails.LinkErrorTo(logErrors);
            destDispenseBatchDetail.LinkErrorTo(logErrors);

            // Trả về các component chính của pipeline để lớp cơ sở có thể theo dõi
            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destDispenseBatchDetail],
                ErrorDestination: logErrors
            );
        }

        #region Transformation Logic Specific to PatientOrders

        private RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndNormalizeOrderItems(HashSet<string> keepAsObjectFields)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow => FlattenAndNormalizeItems(parentRow, keepAsObjectFields));
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateDispenseBatchDetailsTransformation(HashSet<string> cols)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(itemRow => FlattenAndTransformDispenseBatchDetail(itemRow, cols));
        }

        private static IEnumerable<ExpandoObject> FlattenAndNormalizeItems(ExpandoObject parentRow, HashSet<string> keepAsObjectFields)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            if (parentAsDict.TryGetValue("_id", out var parentId) && parentId is ObjectId poid)
            {
                parentAsDict["_id"] = poid.ToString();
            }

            if (parentAsDict.TryGetValue("patientorderitems", out object? value) && value is IEnumerable<object> orderItems)
            {
                foreach (object? sourceItem in orderItems)
                {
                    if (sourceItem == null) continue;
                    var newItem = new ExpandoObject();
                    var newItemDict = (IDictionary<string, object?>)newItem;
                    var sourceItemDict = (IDictionary<string, object?>)sourceItem;
                    foreach (var key in sourceItemDict.Keys)
                    {
                        DataTransformer.MapProperty(sourceItemDict, newItemDict, key, keepAsObjectFields);
                    }
                    // Logic nghiệp vụ cụ thể: Thêm khóa ngoại
                    newItemDict["patientordersuid"] = parentAsDict["_id"];
                    yield return newItem;
                }
            }
        }

        private static IEnumerable<ExpandoObject> FlattenAndTransformDispenseBatchDetail(ExpandoObject poItemRow, ICollection<string> cols)
        {
            var poItemDict = (IDictionary<string, object?>)poItemRow;
            if (poItemDict.TryGetValue("dispensebatchdetail", out object? val) && val is IEnumerable<object> details)
            {
                foreach (object? detail in details)
                {
                    if (detail == null) continue;
                    // Ở cấp độ này, không có mảng con nào cần giữ lại, nên không cần truyền config
                    var targetDetail = DataTransformer.TransformObject((ExpandoObject)detail, cols);
                    var targetDict = (IDictionary<string, object?>)targetDetail;
                    // Logic nghiệp vụ cụ thể: Thêm khóa ngoại
                    targetDict["patientorderitemsuid"] = poItemDict["_id"];
                    yield return targetDetail;
                }
            }
        }

        #endregion
    }
}
