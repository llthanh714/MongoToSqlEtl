using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.MongoDb;
using ETLBox.SqlServer;
using MongoDB.Bson;
using MongoDB.Driver;
using Serilog;
using System.Collections;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl
{
    public class Program
    {
        public static async Task Main()
        {
            // STEP 1: Cấu hình Serilog
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .WriteTo.File("logs/etl-log-.txt",
                    rollingInterval: RollingInterval.Day,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            try
            {
                Log.Information("--- BẮT ĐẦU PHIÊN LÀM VIỆC ETL ---");
                await RunEtlProcess();
                Log.Information("--- KẾT THÚC PHIÊN LÀM VIỆC ETL THÀNH CÔNG ---");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Ứng dụng ETL đã gặp lỗi nghiêm trọng và bị dừng lại.");
                // Ném lại lỗi để trả về một exit code khác 0, báo hiệu cho hệ thống bên ngoài là đã có lỗi.
                throw;
            }
            finally
            {
                await Log.CloseAndFlushAsync();
            }
        }

        private static async Task RunEtlProcess()
        {
            // 1. Cấu hình và kết nối
            var config = LoadConfiguration();
            var sqlConnectionManager = CreateSqlConnectionManager(config);
            var mongoClient = CreateMongoDbClient(config);

            // 2. Lấy schema bảng đích
            var patientordersDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorders");
            var patientorderitemsDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorderitems");
            var dispensebatchdetailDef = TableDefinition.FromTableName(sqlConnectionManager, "dispensebatchdetail");

            // 3. Khởi tạo các thành phần ETL
            var source = CreateMongoDbSource(mongoClient);
            var logErrors = CreateErrorLoggingDestination(); // Destination để ghi log lỗi

            // Các bước transformation
            var transformPatientOrders = CreateTransformComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var flattenAndNormalizeOrderItems = CreateFlattenAndNormalizeOrderItems();
            var transformOrderItemForSql = CreateTransformComponent([.. patientorderitemsDef.Columns.Select(c => c.Name)]);
            var flattenDispenseBatchDetails = CreateDispenseBatchDetailsTransformation([.. dispensebatchdetailDef.Columns.Select(c => c.Name)]);

            // Destinations
            var destPatientOrders = CreateDbMergeDestination(sqlConnectionManager, "patientorders");
            var destPatientOrderItems = CreateDbMergeDestination(sqlConnectionManager, "patientorderitems");
            var destDispenseBatchDetail = CreateDbMergeDestination(sqlConnectionManager, "dispensebatchdetail");

            // 4. Xây dựng và liên kết pipeline
            BuildAndLinkPipeline(source, logErrors, transformPatientOrders, flattenAndNormalizeOrderItems, transformOrderItemForSql, flattenDispenseBatchDetails, destPatientOrders, destPatientOrderItems, destDispenseBatchDetail);

            // 5. Thực thi
            Log.Information("Bắt đầu thực thi Network...");
            await Network.ExecuteAsync(source);
            Log.Information("Network đã thực thi xong.");
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
            Log.Information("Lấy dữ liệu từ MongoDB được chỉnh sửa sau: {StartDate}", startDate);
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

        private static CustomDestination<ETLBoxError> CreateErrorLoggingDestination()
        {
            return new CustomDestination<ETLBoxError>
            {
                WriteAction = (error, rowIndex) =>
                {
                    Log.Warning("Lỗi Dòng Dữ Liệu: {@ErrorRecord}. Nguyên nhân: {ErrorMessage}", error.RecordAsJson, error.GetException().Message);
                }
            };
        }

        #endregion

        #region STEP 3: Transformation Logic
        private static IEnumerable<ExpandoObject> FlattenAndNormalizeItems(ExpandoObject parentRow)
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
                        MapProperty(sourceItemDict, newItemDict, key);
                    }
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
                    if (key == "dispensebatchdetail") { target[key] = value; }
                    else { target[key] = JsonSerializer.Serialize(value); }
                }
                else { target[key] = value; }
            }
            else { target[key] = DBNull.Value; }
        }
        #endregion

        #region STEP 4: Pipeline Linking
        private static void BuildAndLinkPipeline(
            MongoDbSource<ExpandoObject> source,
            CustomDestination<ETLBoxError> logErrors,
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
            flattenDispenseBatchDetails.LinkTo(destDispenseBatchDetails);
            flattenDispenseBatchDetails.LinkErrorTo(logErrors);
            destDispenseBatchDetails.LinkErrorTo(logErrors);
        }
        #endregion
    }
}
