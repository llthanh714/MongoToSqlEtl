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
            // Get configuration from appsettings.json
            IConfiguration config = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json")
              .Build();

            // Get connection strings with null checks
            string? mongoConnectionString = config.GetConnectionString("MongoDb");
            string? sqlConnectionString = config.GetConnectionString("SqlServer");
            if (string.IsNullOrEmpty(sqlConnectionString))
            {
                throw new InvalidOperationException("SQL Server connection string is not configured in appsettings.json");
            }
            if (string.IsNullOrEmpty(mongoConnectionString))
            {
                throw new InvalidOperationException("MongoDB connection string is not configured in appsettings.json");
            }

            // Connect to SQL Server and get table definitions
            var sqlConnectionManager = new SqlConnectionManager(sqlConnectionString);

            // patientorders table definition
            var patientordersDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorders");
            var patientordersColumns = new HashSet<string>(
                patientordersDef.Columns.Select(col => col.Name),
                StringComparer.OrdinalIgnoreCase // Rất quan trọng khi SQL Server không phân biệt hoa/thường
            );
            // patientorderitems table definition
            var patientorderitemsDef = TableDefinition.FromTableName(sqlConnectionManager, "patientorderitems");
            var patientorderitemsColumns = new HashSet<string>(
                patientorderitemsDef.Columns.Select(col => col.Name),
                StringComparer.OrdinalIgnoreCase // Rất quan trọng khi SQL Server không phân biệt hoa/thường
            );

            // Get data from MongoDB
            var startDate = new DateTime(2025, 7, 7, 0, 0, 0, DateTimeKind.Utc);
            var filter = Builders<BsonDocument>.Filter.Gte("modifiedat", startDate);
            var client = new MongoClient(mongoConnectionString);

            var source = new MongoDbSource<ExpandoObject>
            {
                DbClient = client,
                DatabaseName = "arcusairdb",
                CollectionName = "patientorders",
                Filter = filter,
                FindOptions = new FindOptions
                {
                    BatchSize = 500
                }
            };

            // Create error and load process tables
            //CreateErrorTableTask.Create(sqlConnectionManager, "etlbox_error");
            //LoadProcessTask.CreateTable(sqlConnectionManager, "etlbox_loadprocess");

            // Create destination table definition
            var dest_patientorders = new DbMerge<ExpandoObject>(sqlConnectionManager, "patientorders")
            {
                TableDefinition = patientordersDef,
                MergeMode = MergeMode.InsertsAndUpdates,
                IdColumns =
                [
                    new IdColumn { IdPropertyName = "_id" }
                ],
                BatchSize = 500
            };

            var dest_patientorderitems = new DbMerge<ExpandoObject>(sqlConnectionManager, "patientorderitems")
            {
                TableDefinition = patientorderitemsDef,
                IdColumns =
                [
                    new IdColumn { IdPropertyName = "_id" }
                ],
                MergeMode = MergeMode.InsertsAndUpdates,
                BatchSize = 500
            };

            //var errorHandler = new ActionBlock<ETLBoxError>(error =>
            //{
            //    Console.BackgroundColor = ConsoleColor.Red;
            //    Console.ForegroundColor = ConsoleColor.White;
            //    Console.WriteLine($"\n--- ROW LEVEL ERROR ---");
            //    Console.WriteLine($"MESSAGE: {error.GetException().Message}");
            //    Console.ResetColor();

            //    // In ra chính xác dòng dữ liệu đã gây ra lỗi
            //    // Sử dụng JsonSerializer để hiển thị object cho dễ đọc
            //    try
            //    {
            //        Console.WriteLine($"PROBLEM DATA: {error.RecordAsJson}");
            //    }
            //    catch
            //    {
            //        Console.WriteLine("PROBLEM DATA: (Could not be serialized to JSON)");
            //    }
            //    Console.WriteLine("-----------------------\n");
            //});

            // STEP 1: PATIENTORDERS
            // Create multicast for processing
            var multicast = new Multicast<ExpandoObject>();

            // Processing patientorders
            var rowtransform_patientorders = new RowTransformation<ExpandoObject>(row =>
            {
                var sourceAsDict = (IDictionary<string, object?>)row;
                var targetPatientOrders = new ExpandoObject();
                var targetDict = (IDictionary<string, object?>)targetPatientOrders;

                // --- LOGIC ĐÃ ĐƯỢC SỬA ĐỔI ---
                // Lặp qua danh sách các cột mà BẢNG ĐÍCH yêu cầu (được định nghĩa trong patientordersColumns)
                foreach (var columnName in patientordersColumns)
                {
                    // Gọi MapProperty cho mỗi cột.
                    // MapProperty sẽ tự xử lý việc gán giá trị hoặc DBNull.Value nếu không tìm thấy trong nguồn.
                    MapProperty(sourceAsDict, targetDict, columnName);
                }
                // --------------------------------

                // Đoạn code cũ đã được thay thế bằng khối logic ở trên:
                /*
                // Go through all properties in the source document
                foreach (var property in sourceAsDict)
                {
                    // Check if the property name matches any column in the SQL table
                    if (patientordersColumns.Contains(property.Key))
                    {
                        MapProperty(sourceAsDict, targetDict, property.Key);
                    }
                }
                */

                return targetPatientOrders;
            });

            // Flatten patientorderitems from patientorders
            var flat_patientorderitems = new RowMultiplication<ExpandoObject, ExpandoObject>(row =>
            {
                var extractedPatientOrderItems = new List<ExpandoObject>();
                var parentAsDict = (IDictionary<string, object?>)row;

                if (parentAsDict.TryGetValue("patientorderitems", out object? value) && value is IEnumerable<object> orderItems)
                {
                    foreach (object? sourceItem in orderItems)
                    {
                        var itemAsDict = (IDictionary<string, object?>)sourceItem;
                        var targetLineItem = new ExpandoObject();
                        var targetDict = (IDictionary<string, object?>)targetLineItem;

                        // --- PHẦN SỬA ĐỔI QUAN TRỌNG ---
                        // Lặp qua danh sách các cột mà BẢNG ĐÍCH yêu cầu.
                        foreach (var columnName in patientorderitemsColumns)
                        {
                            // Gọi MapProperty cho MỖI cột mà đích cần.
                            // Hàm MapProperty sẽ tự động xử lý việc gán DBNull.Value 
                            // nếu cột không tồn tại trong nguồn (itemAsDict).
                            MapProperty(itemAsDict, targetDict, columnName);
                        }
                        // ------------------------------------

                        // Console.WriteLine(itemAsDict["_id"]); // Dòng này có thể gây lỗi nếu _id không tồn tại, nên cẩn thận

                        // Add foreign key to link back to the parent patientorder
                        targetDict["patientordersuid"] = parentAsDict["_id"];

                        extractedPatientOrderItems.Add(targetLineItem);
                    }
                }
                return extractedPatientOrderItems;
            });

            // STEP 2: PATIENTORDERITEMS
            // Multicast for patientorderitems
            var multicast_patientorderitems = new Multicast<ExpandoObject>();

            // Flatten patientorderitems to prepare for insertion
            var rowtransform_patientorderitems = new RowTransformation<ExpandoObject>(row =>
            {
                var sourceAsDict = (IDictionary<string, object?>)row;
                var targetOrderItem = new ExpandoObject();
                var targetDict = (IDictionary<string, object?>)targetOrderItem;

                // Go through all properties in the dest document
                foreach (var columnName in patientorderitemsColumns)
                {
                    // Gọi MapProperty cho mỗi cột.
                    // MapProperty sẽ tự xử lý việc gán giá trị hoặc DBNull.Value nếu không tìm thấy trong nguồn.
                    MapProperty(sourceAsDict, targetDict, columnName);
                }

                //foreach (var property in sourceAsDict)
                //{
                //    // Check if the property name matches any column in the SQL table
                //    if (patientorderitemsColumns.Contains(property.Key))
                //    {
                //        MapProperty(sourceAsDict, targetDict, property.Key);
                //    }
                //}

                return targetOrderItem;
            });


            // Nhánh 2.2: Làm phẳng dispensebatchdetail
            //var flat_patientorderitems_dispensebatchdetail = new RowMultiplication<ExpandoObject, ExpandoObject>(row =>
            //{
            //    dynamic parentPatientOrderItem = row;
            //    var extractedDetails = new List<ExpandoObject>();
            //    var parentAsDict = (IDictionary<string, object>)parentPatientOrderItem;

            //    if (parentAsDict.ContainsKey("dispensebatchdetail") && parentAsDict["dispensebatchdetail"] is IEnumerable<object> details)
            //    {
            //        foreach (dynamic sourceDetail in details)
            //        {
            //            var targetDetail = new ExpandoObject();
            //            var targetDict = (IDictionary<string, object>)targetDetail;

            //            // Sao chép tất cả thuộc tính của dispensebatchdetail
            //            foreach (var prop in (IDictionary<string, object>)sourceDetail)
            //            {
            //                targetDict[prop.Key] = prop.Value;
            //            }

            //            // Thêm khóa ngoại trỏ về patientorders_patientorderitems
            //            targetDict["patientorderitems_id"] = parentAsDict["_id"];

            //            extractedDetails.Add(targetDetail);
            //        }
            //    }
            //    return extractedDetails;
            //});

            //STEP 3: CONNECTING DATA FLOW
            source.LinkTo(multicast);
            // With this corrected code:
            // dest_patientorders.LinkErrorTo();

            // -- Liên kết giai đoạn 1 --
            multicast.LinkTo(rowtransform_patientorders);
            rowtransform_patientorders.LinkTo(dest_patientorders);
            multicast.LinkTo(flat_patientorderitems);

            // -- Liên kết giai đoạn 2 --
            flat_patientorderitems.LinkTo(multicast_patientorderitems);
            multicast_patientorderitems.LinkTo(rowtransform_patientorderitems);
            rowtransform_patientorderitems.LinkTo(dest_patientorderitems);

            //mc_patientorderitems.LinkTo(flat_patientorderitems_dispensebatchdetail);
            //flat_patientorderitems_dispensebatchdetail.LinkTo(dest_patientorderitems_dispensebatchdetail);

            //STEP 4: EXECUTE ETL
            Console.WriteLine("Start the ETL process...");
            await Network.ExecuteAsync(source);
            Console.WriteLine("Done");
        }

        /// <summary>
        /// Mapping properties from source dictionary to target dictionary.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="target"></param>
        /// <param name="key"></param>
        private static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key)
        {
            // Kiểm tra xem key có tồn tại trong dictionary nguồn không
            if (source.TryGetValue(key, out object? value))
            {
                // Xử lý trường hợp giá trị là null ngay từ đầu
                if (value == null)
                {
                    target[key] = DBNull.Value;
                    return; // Kết thúc sớm
                }

                // 1. Nếu giá trị là kiểu DateTime
                if (value is DateTime utcDateTime)
                {
                    // Chuyển từ UTC sang UTC+7 (SE Asia Standard Time)
                    try
                    {
                        var vietnamTime = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"));
                        target[key] = vietnamTime;
                    }
                    catch (TimeZoneNotFoundException)
                    {
                        target[key] = utcDateTime.AddHours(7); // Fallback khi không tìm thấy TimeZone ID
                    }
                }
                // 2. Nếu giá trị là kiểu ObjectId
                else if (value is ObjectId objectId)
                {
                    // Tự động chuyển ObjectId thành chuỗi (string)
                    target[key] = objectId.ToString();
                }
                // 3. PHẦN MỚI: Nếu giá trị là một mảng/danh sách (và không phải là một chuỗi)
                else if (value is IEnumerable && value is not string)
                {
                    // Chuyển đổi toàn bộ mảng/danh sách thành một chuỗi JSON
                    target[key] = JsonSerializer.Serialize(value);
                }
                // 4. Với các kiểu dữ liệu khác (string, int, double, bool, object lồng nhau...)
                else
                {
                    // Giữ nguyên giá trị gốc
                    target[key] = value;
                }
            }
            else
            {
                // Nếu thuộc tính không tồn tại trong nguồn, gán DBNull.Value để SQL hiểu là NULL
                target[key] = DBNull.Value;
            }
        }
    }
}