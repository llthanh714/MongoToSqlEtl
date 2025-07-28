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

        protected override List<string> StagingTables =>
        [
            "stg_patientorders",
            "stg_patientorderitems",
            "stg_patientdiagnosisuids"
        ];

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

            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination(context);

            var transformPatientOrders = DataTransformer.CreateTransformComponent(
                [.. patientordersDef.Columns.Select(c => c.Name)]);

            var flattenAndTransformOrderItems = CreateFlattenAndTransformComponent(
                [.. patientorderitemsDef.Columns.Select(c => c.Name)],
                null,
                "patientorderitems",
                "patientordersuid"
            );

            var flattenAndTransformDiagnosisUids = CreateFlattenAndTransformComponent(
                [.. patientdiagnosisuidsDef.Columns.Select(c => c.Name)],
                null,
                "patientdiagnosisuids",
                "patientordersuid"
            );

            var destPatientOrders = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestPatientOrdersTable };
            var destPatientOrderItems = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestPatientOrderItemsTable };
            var destPatientDiagnosisUids = new DbDestination<ExpandoObject>() { ConnectionManager = SqlConnectionManager, TableName = DestPatientDiagnosisuidsTable };

            var multicastOrders = new Multicast<ExpandoObject>();

            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // Flow 1: patientorders
            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);
            transformPatientOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            // Flow 2: patientorderitems
            multicastOrders.LinkTo(flattenAndTransformOrderItems,
                o => ((IDictionary<string, object?>)o).ContainsKey("patientorderitems"));
            flattenAndTransformOrderItems.LinkTo(destPatientOrderItems);
            flattenAndTransformOrderItems.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            // Flow 3: patientdiagnosisuids
            multicastOrders.LinkTo(flattenAndTransformDiagnosisUids,
                o => ((IDictionary<string, object?>)o).ContainsKey("patientdiagnosisuids"));
            flattenAndTransformDiagnosisUids.LinkTo(destPatientDiagnosisUids);
            flattenAndTransformDiagnosisUids.LinkErrorTo(logErrors);
            destPatientDiagnosisUids.LinkErrorTo(logErrors);

            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destPatientDiagnosisUids],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: "sp_MergePatientOrdersData"
            );
        }

        #region Transformation Logic Specific to PatientOrders

        /// <summary>
        /// Create a transformation component that flattens and transforms the data from the source collection.
        /// </summary>
        /// <param name="targetColumns"></param>
        /// <param name="keepAsObjectFields"></param>
        /// <param name="arrayFieldName"></param>
        /// <param name="foreignKeyName"></param>
        /// <returns></returns>
        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndTransformComponent(ICollection<string> targetColumns, HashSet<string>? keepAsObjectFields, string arrayFieldName, string foreignKeyName)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(parentRow =>
            {
                var results = new List<ExpandoObject>();
                var parentAsDict = (IDictionary<string, object?>)parentRow;

                // Lấy foreign key từ bản ghi cha
                if (!parentAsDict.TryGetValue("_id", out var parentIdValue))
                {
                    return results; // Bỏ qua nếu không có ID cha
                }
                var parentIdAsString = parentIdValue?.ToString() ?? string.Empty;

                // Kiểm tra và xử lý mảng
                if (parentAsDict.TryGetValue(arrayFieldName, out object? value) && value is IEnumerable<object> items)
                {
                    foreach (object? sourceItem in items)
                    {
                        if (sourceItem == null) continue;

                        var itemAsExpando = (sourceItem is ExpandoObject expando)
                            ? expando
                            : ConvertToExando(sourceItem);

                        if (itemAsExpando == null) continue;

                        // --- LOGIC TỐI ƯU HÓA BẮT ĐẦU TỪ ĐÂY ---
                        // Thay vì gọi DataTransformer.TransformObject, chúng ta thực hiện logic biến đổi tại chỗ.
                        var sourceItemDict = (IDictionary<string, object?>)itemAsExpando;
                        var finalItemDict = (IDictionary<string, object?>)new ExpandoObject();

                        // 1. Map các thuộc tính dựa trên các cột mục tiêu (targetColumns)
                        foreach (var columnName in targetColumns)
                        {
                            // Bỏ qua foreign key vì nó sẽ được thêm ở bước sau
                            if (columnName.Equals(foreignKeyName, StringComparison.OrdinalIgnoreCase))
                                continue;

                            // "Inline" logic từ DataTransformer.MapProperty
                            DataTransformer.MapProperty(sourceItemDict, finalItemDict, columnName, keepAsObjectFields);
                        }

                        // 2. Thêm foreign key vào đối tượng cuối cùng
                        finalItemDict[foreignKeyName] = parentIdAsString;

                        results.Add((ExpandoObject)finalItemDict);
                        // --- LOGIC TỐI ƯU HÓA KẾT THÚC ---
                    }
                }
                return results;
            });
        }

        /// <summary>
        /// Convert một đối tượng thành ExpandoObject.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
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
                Log.Warning(ex, "Could not convert object to ExpandoObject. Object Type: {ObjectType}", obj?.GetType().FullName ?? "null");
                return null;
            }
        }

        #endregion
    }
}