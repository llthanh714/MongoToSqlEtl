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

            // With this corrected line:



            // Tạo một HashSet chứa tên các cột để tra cứu nhanh hơn và không phân biệt chữ hoa/thường


            // Tạo một HashSet chứa tên các cột để tra cứu nhanh hơn và không phân biệt chữ hoa/thường


            // 2. ĐỊNH NGHĨA CÁC THÀNH PHẦN
            // Nguồn MongoDB
            var startDate = new DateTime(2025, 7, 6, 0, 0, 0, DateTimeKind.Utc);
            var filter = Builders<BsonDocument>.Filter.Gte("modifiedat", startDate);
            var source = new MongoDbSource<ExpandoObject>
            {
                ConnectionString = mongoConnectionString,
                DatabaseName = "arcusairdb",
                CollectionName = "patientorders",
                Filter = filter,
                FindOptions = new FindOptions
                {
                    BatchSize = 500
                }
            };

            var dest_patientorders = new DbMerge<ExpandoObject>(sqlConnectionManager, "patientorders")
            {
                TableDefinition = tableDef,
                MergeMode = MergeMode.InsertsAndUpdates,
                IdColumns =
                [
                    new IdColumn { IdPropertyName = "_id" }
                ],
                //MergeMode = MergeMode.InsertsAndUpdates,
                BatchSize = 500 // Giới hạn kích thước lô để tránh quá tải
            };

            var dest_patientorders_patientorderitems = new DbMerge<ExpandoObject>(sqlConnectionManager, "patientorderitems")
            {
                TableDefinition = patientorderitemsDef,
                IdColumns =
                [
                    new IdColumn { IdPropertyName = "_id" }
                ],
                MergeMode = MergeMode.InsertsAndUpdates,
                BatchSize = 500 // Giới hạn kích thước lô để tránh quá tải
            };

            //var dest_patientorderitems_dispensebatchdetail = new DbMerge<ExpandoObject>(sqlConnectionManager, "patientorderitems_dispensebatchdetail")
            //{
            //    MergeMode = MergeMode.Delta,
            //    BatchSize = 500 // Giới hạn kích thước lô để tránh quá tải
            //};

            // --- GIAI ĐOẠN 1: patientorders -> patientorderitems ---

            var mc_Invoice = new Multicast<ExpandoObject>();

            // Nhánh 1.1: Xử lý và ghi vào bảng patientorders
            var proc_patientorders = new RowTransformation<ExpandoObject>(row =>
            {
                var sourceAsDict = (IDictionary<string, object>)row;
                var targetOrder = new ExpandoObject();
                var targetDict = (IDictionary<string, object>)targetOrder;

                // Duyệt qua tất cả các thuộc tính có trong document từ MongoDB
                foreach (var property in sourceAsDict)
                {
                    // Kiểm tra xem tên thuộc tính có khớp với một cột nào trong bảng SQL không
                    if (patientordersColumns.Contains(property.Key))
                    {
                        MapProperty(sourceAsDict, targetDict, property.Key);
                    }
                }

                return targetOrder;
            });

            //Nhánh 1.2: Làm phẳng patientorderitems
            var flat_patientorderitems = new RowMultiplication<ExpandoObject, ExpandoObject>(row =>
            {
                dynamic parentDoc = row;
                var extractedPatientOrderItems = new List<ExpandoObject>();
                var parentAsDict = (IDictionary<string, object>)parentDoc;

                if (parentAsDict.ContainsKey("patientorderitems") && parentAsDict["patientorderitems"] is IEnumerable<object> items)
                {
                    foreach (object sourceItem in items)
                    {
                        var itemAsDict = (IDictionary<string, object>)sourceItem;
                        var targetLineItem = new ExpandoObject();
                        var targetDict = (IDictionary<string, object>)targetLineItem;

                        // Sao chép tất cả thuộc tính của patientorderitems
                        foreach (var prop in itemAsDict)
                        {
                            if (patientorderitemsColumns.Contains(prop.Key))
                            {
                                MapProperty(itemAsDict, targetDict, prop.Key);
                            }

                            //targetDict[prop.Key] = prop.Value;
                        }

                        // Thêm khóa ngoại trỏ về patientorders
                        targetDict["patientordersuid"] = parentAsDict["_id"];

                        extractedPatientOrderItems.Add(targetLineItem);
                    }
                }
                return extractedPatientOrderItems;
            });


            // --- GIAI ĐOẠN 2: patientorderitems -> dispensebatchdetail ---

            var mc_patientorderitems = new Multicast<ExpandoObject>();

            // Nhánh 2.1: Xử lý và ghi vào bảng patientorders_patientorderitems
            // Trong trường hợp này, flat_patientorderitems đã có đủ dữ liệu, ta có thể không cần proc_patientorderitems
            // và link trực tiếp đến đích nếu không có thêm transformation nào.
            // Tuy nhiên, giữ lại cấu trúc này nếu bạn cần xử lý thêm.

            // With this corrected line:




            var proc_patientorderitems = new RowTransformation<ExpandoObject>(row =>
            {
                var sourceAsDict = (IDictionary<string, object>)row;
                var targetOrderItem = new ExpandoObject();
                var targetDict = (IDictionary<string, object>)targetOrderItem;

                // Duyệt qua tất cả các thuộc tính có trong document từ MongoDB
                foreach (var property in sourceAsDict)
                {
                    // Kiểm tra xem tên thuộc tính có khớp với một cột nào trong bảng SQL không
                    if (patientorderitemsColumns.Contains(property.Key))
                    {
                        MapProperty(sourceAsDict, targetDict, property.Key);
                    }
                }

                return targetOrderItem;


                // Nếu cần transformation thêm cho patientorderitems, hãy viết ở đây.
                // Nếu không, chỉ cần trả về đối tượng gốc.
                //return row;
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

            // 3. LIÊN KẾT CÁC THÀNH PHẦN
            source.LinkTo(mc_Invoice);

            // -- Liên kết giai đoạn 1 --
            mc_Invoice.LinkTo(proc_patientorders);
            proc_patientorders.LinkTo(dest_patientorders);

            mc_Invoice.LinkTo(flat_patientorderitems);

            // -- Liên kết giai đoạn 2 --
            flat_patientorderitems.LinkTo(mc_patientorderitems);

            mc_patientorderitems.LinkTo(proc_patientorderitems);
            proc_patientorderitems.LinkTo(dest_patientorders_patientorderitems);

            //mc_patientorderitems.LinkTo(flat_patientorderitems_dispensebatchdetail);
            //flat_patientorderitems_dispensebatchdetail.LinkTo(dest_patientorderitems_dispensebatchdetail);

            // 4. THỰC THI
            Console.WriteLine("Bắt đầu quá trình ETL đa cấp...");

            await Network.ExecuteAsync(source);
            Console.WriteLine("Quá trình ETL đã hoàn tất.");
            Console.WriteLine($"Kiểm tra file 'error_log.csv' nếu có lỗi xảy ra.");
        }

        /// <summary>
        /// Ánh xạ một thuộc tính từ source Dictionary sang target Dictionary.
        /// Xử lý trường hợp thuộc tính không tồn tại và chuyển đổi múi giờ cho DateTime.
        /// </summary>
        private static void MapProperty(IDictionary<string, object> source, IDictionary<string, object> target, string key)
        {
            // Kiểm tra xem key có tồn tại trong dictionary nguồn không
            if (source.TryGetValue(key, out object value))
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