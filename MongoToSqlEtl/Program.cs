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
        // STEP 5: EXECUTE ETL PROCESS
        public static async Task Main(string[] args)
        {
            ArgumentNullException.ThrowIfNull(args);

            // 1. Tải cấu hình và kết nối
            var config = LoadConfiguration();
            var sqlConnectionManager = CreateSqlConnectionManager(config);
            var mongoClient = CreateMongoDbClient(config);

            // 2. Lấy định nghĩa (schema) của các bảng SQL đích
            var patientordersDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorders");
            var patientorderitemsDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorderitems");
            var dispensebatchdetailDef = TableDefinition.FromTableName(sqlConnectionManager, "dispensebatchdetail");
            var patientordersColumns = new HashSet<string>(patientordersDef.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
            var patientorderitemsColumns = new HashSet<string>(patientorderitemsDef.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
            var dispensebatchdetailColumns = new HashSet<string>(dispensebatchdetailDef.Columns.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);

            // 3. Khởi tạo các thành phần ETLBox (Source, Transformations, Destinations)
            var source = CreateMongoDbSource(mongoClient);
            var (transformPatientOrders, flattenOrderItems) = CreateTransformations(patientordersColumns, patientorderitemsColumns);
            var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation(dispensebatchdetailColumns);

            var destPatientOrders = CreateDbMergeDestination(sqlConnectionManager, patientordersDef, "patientorders");
            var destPatientOrderItems = CreateDbMergeDestination(sqlConnectionManager, patientorderitemsDef, "patientorderitems");
            var destDispenseBatchDetail = CreateDbMergeDestination(sqlConnectionManager, dispensebatchdetailDef, "dispensebatchdetail");

            // 4. Xây dựng và liên kết các luồng dữ liệu (Data Flow Pipeline)
            BuildAndLinkPipeline(source, transformPatientOrders, flattenOrderItems, flattenDispenseBatchDetails, destPatientOrders, destPatientOrderItems, destDispenseBatchDetail);

            Console.WriteLine("Starting ETL process...");
            await Network.ExecuteAsync(source);
            Console.WriteLine("Completed.");
        }

        #region STEP 1: Configuration and Connection Setup

        private static IConfiguration LoadConfiguration()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
        }

        private static SqlConnectionManager CreateSqlConnectionManager(IConfiguration config)
        {
            string? sqlConnectionString = config.GetConnectionString("SqlServer");
            if (string.IsNullOrEmpty(sqlConnectionString))
            {
                throw new InvalidOperationException("Connection string 'SqlServer' chưa được cấu hình trong appsettings.json");
            }
            return new SqlConnectionManager(sqlConnectionString);
        }

        private static MongoClient CreateMongoDbClient(IConfiguration config)
        {
            string? mongoConnectionString = config.GetConnectionString("MongoDb");
            if (string.IsNullOrEmpty(mongoConnectionString))
            {
                throw new InvalidOperationException("Connection string 'MongoDb' chưa được cấu hình trong appsettings.json");
            }
            return new MongoClient(mongoConnectionString);
        }

        #endregion

        #region STEP 2: ETL Component Creation

        private static MongoDbSource<ExpandoObject> CreateMongoDbSource(MongoClient client)
        {
            var startDate = DateTime.Now.AddHours(-3); //new DateTime(2025, 7, 9, 0, 0, 0, DateTimeKind.Utc);
            var filter = Builders<BsonDocument>.Filter.Gte("modifiedat", startDate);

            return new MongoDbSource<ExpandoObject>
            {
                DbClient = client,
                DatabaseName = "arcusairdb",
                CollectionName = "patientorders",
                Filter = filter,
                FindOptions = new FindOptions { BatchSize = 500 }
            };
        }

        private static DbMerge<ExpandoObject> CreateDbMergeDestination(IConnectionManager connectionManager, TableDefinition tableDef, string tableName)
        {
            return new DbMerge<ExpandoObject>(connectionManager, tableName)
            {
                TableDefinition = tableDef,
                MergeMode = MergeMode.InsertsAndUpdates,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }],
                BatchSize = 500
            };
        }

        private static (RowTransformation<ExpandoObject>, RowMultiplication<ExpandoObject, ExpandoObject>) CreateTransformations(
            HashSet<string> patientordersColumns,
            HashSet<string> patientorderitemsColumns)
        {
            var transformPatientOrders = new RowTransformation<ExpandoObject>(
                row => TransformPatientOrder(row, patientordersColumns)
            );

            var flattenOrderItems = new RowMultiplication<ExpandoObject, ExpandoObject>(
                row => FlattenAndTransformOrderItems(row, patientorderitemsColumns)
            );

            return (transformPatientOrders, flattenOrderItems);
        }

        private static RowMultiplication<ExpandoObject, ExpandoObject> CreateDispenseBatchDetailsTransformation(
            HashSet<string> dispensebatchdetailsColumns)
        {
            return new RowMultiplication<ExpandoObject, ExpandoObject>(
                itemRow => FlattenAndTransformDispenseBatchDetail(itemRow, dispensebatchdetailsColumns)
            );
        }


        #endregion

        #region STEP 3: Transformation Logic

        private static ExpandoObject TransformPatientOrder(ExpandoObject row, ICollection<string> targetColumns)
        {
            var sourceAsDict = (IDictionary<string, object?>)row;
            var targetDict = (IDictionary<string, object?>)new ExpandoObject();

            foreach (var columnName in targetColumns)
            {
                MapProperty(sourceAsDict, targetDict, columnName);
            }

            return (ExpandoObject)targetDict;
        }

        private static IEnumerable<ExpandoObject> FlattenAndTransformOrderItems(ExpandoObject parentRow, ICollection<string> targetItemColumns)
        {
            var parentAsDict = (IDictionary<string, object?>)parentRow;

            if (parentAsDict.TryGetValue("patientorderitems", out object? value) && value is IEnumerable<object> orderItems)
            {
                foreach (object? sourceItem in orderItems)
                {
                    if (sourceItem == null) continue;

                    var itemAsDict = (IDictionary<string, object?>)sourceItem;
                    var targetLineItem = new ExpandoObject();
                    var targetDict = (IDictionary<string, object?>)targetLineItem;

                    // Lặp qua các cột mà bảng đích patientorderitems yêu cầu
                    foreach (var columnName in targetItemColumns)
                    {
                        MapProperty(itemAsDict, targetDict, columnName);
                    }

                    // Thêm khóa ngoại để liên kết về patientorder cha
                    targetDict["patientordersuid"] = parentAsDict["_id"];

                    yield return targetLineItem; // Sử dụng yield return để trả về từng item một cách hiệu quả
                }
            }
        }

        private static IEnumerable<ExpandoObject> FlattenAndTransformDispenseBatchDetail(ExpandoObject patientOrderItemRow, ICollection<string> targetDetailColumns)
        {
            var patientOrderItemAsDict = (IDictionary<string, object?>)patientOrderItemRow;

            if (patientOrderItemAsDict.TryGetValue("dispensebatchdetail", out object? value) && value is IEnumerable<object> dispenseDetails)
            {
                foreach (object? sourceDetail in dispenseDetails)
                {
                    if (sourceDetail == null) continue;

                    var detailAsDict = (IDictionary<string, object?>)sourceDetail;
                    var targetDetailItem = new ExpandoObject();
                    var targetDict = (IDictionary<string, object?>)targetDetailItem;

                    foreach (var columnName in targetDetailColumns)
                    {
                        MapProperty(detailAsDict, targetDict, columnName);
                    }

                    targetDict["patientorderitemsuid"] = patientOrderItemAsDict["_id"];
                    yield return targetDetailItem;
                }
            }
        }

        private static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key)
        {
            if (source.TryGetValue(key, out object? value))
            {
                if (value == null)
                {
                    target[key] = DBNull.Value;
                    return;
                }

                if (value is DateTime utcDateTime)
                {
                    try
                    {
                        var vietnamTime = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"));
                        target[key] = vietnamTime;
                    }
                    catch (TimeZoneNotFoundException)
                    {
                        target[key] = utcDateTime.AddHours(7); // Fallback
                    }
                }
                else if (value is ObjectId objectId)
                {
                    target[key] = objectId.ToString();
                }
                else if (value is IEnumerable && value is not string)
                {
                    target[key] = JsonSerializer.Serialize(value);
                }
                else
                {
                    target[key] = value;
                }
            }
            else
            {
                target[key] = DBNull.Value;
            }
        }

        #endregion

        #region STEP 4: Pipeline Linking

        private static void BuildAndLinkPipeline(
            MongoDbSource<ExpandoObject> source,
            RowTransformation<ExpandoObject> transformPatientOrders,
            RowMultiplication<ExpandoObject, ExpandoObject> flattenOrderItems,
            RowMultiplication<ExpandoObject, ExpandoObject> flattenDispenseBatchDetails,
            DbMerge<ExpandoObject> destPatientOrders,
            DbMerge<ExpandoObject> destPatientOrderItems,
            DbMerge<ExpandoObject> destDispenseBatchDetails)
        {
            var multicastOrders = new Multicast<ExpandoObject>();
            var multicastOrderItems = new Multicast<ExpandoObject>();

            // Từ source, chia làm 2 nhánh (patientorders và patientorderitems)
            source.LinkTo(multicastOrders);

            // Nhánh 1: Xử lý patientorders và đưa vào bảng patientorders
            multicastOrders.LinkTo(transformPatientOrders);
            transformPatientOrders.LinkTo(destPatientOrders);

            // Nhánh 2: Làm phẳng patientorderitems
            multicastOrders.LinkTo(flattenOrderItems);

            // From flattened patientorderitems
            flattenOrderItems.LinkTo(multicastOrderItems);

            // Nhánh 2.1: Đưa patientorderitems vào bảng patientorderitems
            multicastOrderItems.LinkTo(destPatientOrderItems);

            // Nhánh 2.2: Làm phẳng dispensebatchdetail và đưa vào bảng dispensebatchdetails
            multicastOrderItems.LinkTo(flattenDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetails);
        }

        #endregion
    }
}