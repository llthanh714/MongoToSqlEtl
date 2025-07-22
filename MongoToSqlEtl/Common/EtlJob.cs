using ETLBox;
using ETLBox.DataFlow;
using MongoDB.Bson;
using System.Collections;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl.Common
{
    /// <summary>
    /// Lớp tiện ích chứa các phương thức biến đổi dữ liệu dùng chung.
    /// </summary>
    public static class DataTransformer
    {
        public static DbMerge<ExpandoObject> CreateDbMergeDestination(IConnectionManager conn, string tableName)
        {
            return new DbMerge<ExpandoObject>(conn, tableName)
            {
                MergeMode = MergeMode.InsertsOnly,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }],
                // BatchSize = 500
            };
        }

        public static RowTransformation<ExpandoObject> CreateTransformComponent(HashSet<string> targetColumns, HashSet<string>? keepAsObjectFields = null)
        {
            return new RowTransformation<ExpandoObject>(row => TransformObject(row, targetColumns, keepAsObjectFields));
        }

        public static ExpandoObject TransformObject(ExpandoObject sourceRow, ICollection<string> targetColumns, HashSet<string>? keepAsObjectFields = null)
        {
            var sourceAsDict = (IDictionary<string, object?>)sourceRow;
            var targetDict = (IDictionary<string, object?>)new ExpandoObject();

            // If target columns are specified, map only those.
            if (targetColumns.Count != 0)
            {
                foreach (var columnName in targetColumns)
                {
                    MapProperty(sourceAsDict, targetDict, columnName, keepAsObjectFields);
                }
            }
            // Otherwise, map all columns from the source.
            else
            {
                foreach (var key in sourceAsDict.Keys)
                {
                    MapProperty(sourceAsDict, targetDict, key, keepAsObjectFields);
                }
            }

            // Always ensure the _id column is mapped if it exists in the source
            if (sourceAsDict.ContainsKey("_id") && !targetDict.ContainsKey("_id"))
            {
                MapProperty(sourceAsDict, targetDict, "_id", keepAsObjectFields);
            }
            return (ExpandoObject)targetDict;
        }

        // Dùng Lazy<T> để đảm bảo việc khởi tạo chỉ xảy ra một lần và thread-safe.
        private static readonly Lazy<TimeZoneInfo> SEAsiaTimeZone = new(() =>
        {
            try
            {
                // Thử tìm TimeZone ID của Windows trước
                return TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time");
            }
            catch (TimeZoneNotFoundException)
            {
                // Nếu không được, thử tìm IANA ID (dùng trên Linux/macOS)
                try
                {
                    return TimeZoneInfo.FindSystemTimeZoneById("Asia/Ho_Chi_Minh");
                }
                catch (TimeZoneNotFoundException)
                {
                    // Nếu cả hai đều thất bại, tạo một múi giờ tùy chỉnh UTC+7 làm phương án cuối cùng.
                    return TimeZoneInfo.CreateCustomTimeZone("SE Asia Custom", TimeSpan.FromHours(7), "SE Asia Standard Time (Custom)", "SE Asia Standard Time (Custom)");
                }
            }
        });

        public static void MapProperty(
            IDictionary<string, object?> source,
            IDictionary<string, object?> target,
            string key,
            HashSet<string>? keepAsObjectFields = null)
        {
            if (target.ContainsKey(key))
            {
                return;
            }
            else
            {
                if (!source.TryGetValue(key, out object? value))
                {
                    target[key] = DBNull.Value;
                    return;
                }

                // Sử dụng switch expression (C# 8.0+) để code sạch và dễ đọc hơn
                target[key] = value switch
                {
                    null => DBNull.Value,

                    DateTime utcDateTime => TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, SEAsiaTimeZone.Value),

                    ObjectId oid => oid.ToString(),

                    string str => str, // Xử lý riêng trường hợp string để không bị coi là IEnumerable

                    IEnumerable enumerable => HandleEnumerable(enumerable, key, keepAsObjectFields),

                    _ => value // Các kiểu dữ liệu nguyên thủy khác (int, bool, double, etc.)
                };
            }
        }

        private static object HandleEnumerable(IEnumerable enumerable, string key, HashSet<string>? keepAsObjectFields)
        {
            // Kiểm tra xem enumerable có phần tử nào không
            var firstItem = enumerable.Cast<object>().FirstOrDefault();
            if (firstItem == null && !enumerable.Cast<object>().Any()) // Double check cho trường hợp list chứa 1 phần tử null
            {
                return DBNull.Value;
            }

            if (keepAsObjectFields != null && keepAsObjectFields.Contains(key))
            {
                // Vẫn giữ logic quan trọng: tạo list mới để phá vỡ tham chiếu
                return enumerable.Cast<object>().ToList();
            }

            // Serialize thành JSON cho các trường hợp còn lại
            return JsonSerializer.Serialize(enumerable);
        }
    }
}