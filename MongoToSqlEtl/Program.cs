using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.MongoDb;
using ETLBox.SqlServer;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);

            // 1. Cấu hình và kết nối
            var config = LoadConfiguration();
            var sqlConnectionManager = CreateSqlConnectionManager(config);
            var mongoClient = CreateMongoDbClient(config);

            // 2. Lấy schema bảng đích
            var patientordersDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorders");
            var patientorderitemsDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorderitems");
            var dispensebatchdetailDef = TableDefinition.FromTableName(sqlConnectionManager, "dispensebatchdetail");
            var patientordersColumns = new HashSet<string>(patientordersDef.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
            var patientorderitemsColumns = new HashSet<string>(patientorderitemsDef.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
            var dispensebatchdetailColumns = new HashSet<string>(dispensebatchdetailDef.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);

            // 3. Khởi tạo các thành phần ETL
            var source = CreateMongoDbSource(mongoClient);

            // Các bước transformation
            var transformPatientOrders = CreateTransformComponent(patientordersColumns);
            // Hàm này sẽ làm phẳng và chuẩn hóa kiểu dữ liệu cho items
            var flattenAndNormalizeOrderItems = CreateFlattenAndNormalizeOrderItems();
            // Hàm này chỉ chọn các cột cần thiết cho SQL
            var transformOrderItemForSql = CreateTransformComponent(patientorderitemsColumns);
            var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation(dispensebatchdetailColumns);

            // Destinations
            var destPatientOrders = CreateDbMergeDestination(sqlConnectionManager, "patientorders");
            var destPatientOrderItems = CreateDbMergeDestination(sqlConnectionManager, "patientorderitems");
            var destDispenseBatchDetail = CreateDbMergeDestination(sqlConnectionManager, "dispensebatchdetail");

            // 4. Xây dựng và liên kết pipeline
            BuildAndLinkPipeline(source, transformPatientOrders, flattenAndNormalizeOrderItems, transformOrderItemForSql, flattenDispenseBatchDetails, destPatientOrders, destPatientOrderItems, destDispenseBatchDetail);

            Console.WriteLine("Starting ETL process...");
            await Network.ExecuteAsync(source);
            Console.WriteLine("Completed.");
        }

        #region STEP 1: Configuration and Connection Setup
        private static IConfiguration LoadConfiguration() => new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
        private static SqlConnectionManager CreateSqlConnectionManager(IConfiguration config)
        {
            string? cs = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("Connection string 'SqlServer' not found.");
            return new SqlConnectionManager(cs);
        }
        private static MongoClient CreateMongoDbClient(IConfiguration config)
        {
            string? cs = config.GetConnectionString("MongoDb") ?? throw new InvalidOperationException("Connection string 'MongoDb' not found.");
            return new MongoClient(cs);
        }
        #endregion

        #region STEP 2: ETL Component Creation
        private static MongoDbSource<ExpandoObject> CreateMongoDbSource(MongoClient client)
        {
            var startDate = DateTime.Now.AddHours(-3);
            var filter = Builders<BsonDocument>.Filter.Gte("modifiedat", startDate);
            return new MongoDbSource<ExpandoObject> { DbClient = client, DatabaseName = "arcusairdb", CollectionName = "patientorders", Filter = filter, FindOptions = new FindOptions { BatchSize = 500 } };
        }

        private static DbMerge<ExpandoObject> CreateDbMergeDestination(IConnectionManager conn, string tableName)
        {
            return new DbMerge<ExpandoObject>(conn, tableName) { MergeMode = MergeMode.InsertsAndUpdates, IdColumns = [new IdColumn { IdPropertyName = "_id" }], BatchSize = 500 };
        }

        private static RowTransformation<ExpandoObject> CreateTransformComponent(HashSet<string> targetColumns)
        {
            return new RowTransformation<ExpandoObject>(row => TransformObject(row, targetColumns));
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateFlattenAndNormalizeOrderItems()
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(FlattenAndNormalizeItems);
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateDispenseBatchDetailsTransformation(HashSet<string> cols)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(itemRow => FlattenAndTransformDispenseBatchDetail(itemRow, cols));
        }
        #endregion

        #region STEP 3: Transformation Logic
        private static IEnumerable<ExpandoObject> FlattenAndNormalizeItems(ExpandoObject parentRow)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;
            // Chuyển đổi _id của cha thành string trước khi dùng làm khóa ngoại
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

                    // Lặp qua tất cả các key của item nguồn để chuẩn hóa dữ liệu
                    foreach (var key in sourceItemDict.Keys)
                    {
                        MapProperty(sourceItemDict, newItemDict, key);
                    }

                    // Thêm khóa ngoại đã được chuẩn hóa
                    newItemDict["patientordersuid"] = parentAsDict["_id"];

                    yield return newItem;
                }
            }
        }

        private static ExpandoObject TransformObject(ExpandoObject sourceRow, ICollection<string> targetColumns)
        {
            var sourceAsDict = (IDictionary<string, object?>)sourceRow;
            var targetDict = (IDictionary<string, object?>)new ExpandoObject();

            foreach (var columnName in targetColumns)
            {
                MapProperty(sourceAsDict, targetDict, columnName);
            }
            if (sourceAsDict.ContainsKey("_id") && !targetDict.ContainsKey("_id")) MapProperty(sourceAsDict, targetDict, "_id");
            if (sourceAsDict.ContainsKey("patientordersuid") && !targetDict.ContainsKey("patientordersuid")) MapProperty(sourceAsDict, targetDict, "patientordersuid");

            return (ExpandoObject)targetDict;
        }

        private static IEnumerable<ExpandoObject> FlattenAndTransformDispenseBatchDetail(ExpandoObject poItemRow, ICollection<string> cols)
        {
            var poItemDict = (IDictionary<string, object?>)poItemRow;
            if (poItemDict.TryGetValue("dispensebatchdetail", out object? val) && val is IEnumerable<object> details)
            {
                foreach (object? detail in details)
                {
                    if (detail == null) continue;
                    var targetDetail = TransformObject((ExpandoObject)detail, cols);
                    var targetDict = (IDictionary<string, object?>)targetDetail;
                    targetDict["patientorderitemsuid"] = poItemDict["_id"];
                    yield return targetDetail;
                }
            }
        }

        private static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key)
        {
            if (source.TryGetValue(key, out object? value))
            {
                if (value == null) { target[key] = DBNull.Value; return; }
                if (value is DateTime utc)
                {
                    try { target[key] = TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time")); }
                    catch (TimeZoneNotFoundException) { target[key] = utc.AddHours(7); }
                }
                else if (value is ObjectId oid) { target[key] = oid.ToString(); }
                else if (value is IEnumerable && value is not string)
                {
                    if (key == "dispensebatchdetail")
                    {
                        target[key] = value;
                    }
                    else
                    {
                        target[key] = JsonSerializer.Serialize(value);
                    }
                }
                else { target[key] = value; }
            }
            else { target[key] = DBNull.Value; }
        }
        #endregion

        #region STEP 4: Pipeline Linking
        private static void BuildAndLinkPipeline(
            MongoDbSource<ExpandoObject> source,
            RowTransformation<ExpandoObject> transformPatientOrders,
            RowMultiplication<ExpandoObject, ExpandoObject> flattenAndNormalizeOrderItems,
            RowTransformation<ExpandoObject> transformOrderItemForSql,
            RowMultiplication<ExpandoObject, ExpandoObject> flattenDispenseBatchDetails,
            DbMerge<ExpandoObject> destPatientOrders,
            DbMerge<ExpandoObject> destPatientOrderItems,
            DbMerge<ExpandoObject> destDispenseBatchDetails)
        {
            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastNormalizedItems = new Multicast<ExpandoObject>();

            // 1. Chia luồng source
            source.LinkTo(multicastOrders);

            // 2. Nhánh 1: Biến đổi và lưu patientorders
            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);

            // 3. Nhánh 2: Làm phẳng và chuẩn hóa kiểu dữ liệu cho patientorderitems
            multicastOrders.LinkTo(flattenAndNormalizeOrderItems);

            // 4. Chia luồng item đã được chuẩn hóa
            flattenAndNormalizeOrderItems.LinkTo(multicastNormalizedItems);

            // 5. Nhánh 2a: Chỉ chọn các cột cần thiết cho SQL và lưu
            multicastNormalizedItems.LinkTo(transformOrderItemForSql);
            transformOrderItemForSql.LinkTo(destPatientOrderItems);

            // 6. Nhánh 2b: Dùng item đã chuẩn hóa để làm phẳng lớp 3
            multicastNormalizedItems.LinkTo(flattenDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetails);
        }
        #endregion
    }
}