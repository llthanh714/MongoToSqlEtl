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
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };
        }

        public static RowTransformation<ExpandoObject> CreateTransformComponent(HashSet<string> targetColumns, HashSet<string>? keepAsObjectFields = null)
        {
            return new RowTransformation<ExpandoObject>(row => TransformObject(row, targetColumns, keepAsObjectFields));
        }

        /// <summary>
        /// Transform an ExpandoObject to another ExpandoObject based on target columns.
        /// </summary>
        /// <param name="sourceRow"></param>
        /// <param name="targetColumns"></param>
        /// <param name="keepAsObjectFields"></param>
        /// <param name="excludeKeys"></param>
        /// <returns></returns>
        public static ExpandoObject TransformObject(ExpandoObject sourceRow, ICollection<string> targetColumns, HashSet<string>? keepAsObjectFields = null, HashSet<string>? excludeKeys = null)
        {
            var sourceAsDict = (IDictionary<string, object?>)sourceRow;
            var targetDict = (IDictionary<string, object?>)new ExpandoObject();

            if (targetColumns.Count != 0)
            {
                foreach (var columnName in targetColumns)
                {
                    if (excludeKeys != null && excludeKeys.Contains(columnName))
                        continue;
                        
                    MapProperty(sourceAsDict, targetDict, columnName, keepAsObjectFields);
                }
            }
            else
            {
                foreach (var key in sourceAsDict.Keys)
                {
                    var exclusionSet = excludeKeys != null
                        ? new HashSet<string>(excludeKeys, StringComparer.OrdinalIgnoreCase)
                        : null;

                    if (exclusionSet != null && exclusionSet.Contains(key))
                        continue;

                    MapProperty(sourceAsDict, targetDict, key, keepAsObjectFields);
                }
            }

            if (sourceAsDict.ContainsKey("_id") && !targetDict.ContainsKey("_id"))
            {
                // Đảm bảo khóa bị loại trừ cũng không được thêm lại ở đây
                if (excludeKeys == null || !excludeKeys.Contains("_id"))
                {
                    MapProperty(sourceAsDict, targetDict, "_id", keepAsObjectFields);
                }
            }
            return (ExpandoObject)targetDict;
        }

        /// <summary>
        /// Using Lazy<T> to initialize the SE Asia Time Zone only when needed.
        /// </summary>
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

        /// <summary>
        /// Handles the conversion of a JsonElement to a database-friendly type.
        /// </summary>
        private static object HandleJsonElement(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    if (element.TryGetDateTime(out var dt))
                        return dt;
                    // FIX: Cast the string to object? to make it compatible with DBNull.Value for the ?? operator.
                    return (object?)element.GetString() ?? DBNull.Value;
                case JsonValueKind.Number:
                    return element.GetDecimal();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                    return DBNull.Value;

                // For complex types, we check for specific BSON patterns or serialize to string.
                case JsonValueKind.Object:
                    // Check for BSON ObjectId pattern: {"$oid": "..."}
                    if (element.TryGetProperty("$oid", out var oidElement))
                    {
                        return (object?)oidElement.GetString() ?? DBNull.Value;
                    }
                    // Check for BSON Date pattern: {"$date": "..."}
                    if (element.TryGetProperty("$date", out var dateElement) && dateElement.TryGetDateTime(out var bsonDate))
                    {
                        return bsonDate;
                    }
                    // If it's another type of object (like the one in the error message for an _id),
                    // convert it to a JSON string to be stored in nvarchar. This solves the InvalidCastException.
                    return element.GetRawText();

                case JsonValueKind.Array:
                    // Convert array to a JSON string.
                    return element.GetRawText();

                default:
                    // Fallback for any other type.
                    return element.ToString();
            }
        }

        /// <summary>
        /// Mapping a property from source to target dictionary.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="target"></param>
        /// <param name="key"></param>
        /// <param name="keepAsObjectFields"></param>
        public static void MapProperty( IDictionary<string, object?> source, IDictionary<string, object?> target, string key, HashSet<string>? keepAsObjectFields = null)
        {
            if (target.ContainsKey(key))
            {
                return;
            }

            if (!source.TryGetValue(key, out object? value))
            {
                target[key] = DBNull.Value;
                return;
            }

            // FIX: The switch expression is updated to handle JsonElement correctly,
            // preventing cast errors when writing to the database.
            target[key] = value switch
            {
                null => DBNull.Value,
                DateTime utcDateTime => TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, SEAsiaTimeZone.Value),
                ObjectId oid => oid.ToString(),
                JsonElement jsonElem => HandleJsonElement(jsonElem), // Handle JsonElement before other types
                string str => str,
                IEnumerable enumerable when value is not string => HandleEnumerable(enumerable, key, keepAsObjectFields),
                _ => value
            };
        }

        /// <summary>
        /// Handles the conversion of an IEnumerable to a database-friendly type.
        /// </summary>
        /// <param name="enumerable"></param>
        /// <param name="key"></param>
        /// <param name="keepAsObjectFields"></param>
        /// <returns></returns>
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