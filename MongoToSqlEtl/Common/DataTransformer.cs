using ETLBox;
using ETLBox.DataFlow;
using MongoDB.Bson;
using System.Collections;
using System.Dynamic;
using System.Text.Json;

namespace MongoToSqlEtl.Common
{
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
                    var exclusionSet = excludeKeys != null ? new HashSet<string>(excludeKeys, StringComparer.OrdinalIgnoreCase) : null;
                    if (exclusionSet != null && exclusionSet.Contains(key))
                        continue;
                    MapProperty(sourceAsDict, targetDict, key, keepAsObjectFields);
                }
            }

            if (sourceAsDict.ContainsKey("_id") && !targetDict.ContainsKey("_id"))
            {
                if (excludeKeys == null || !excludeKeys.Contains("_id"))
                {
                    MapProperty(sourceAsDict, targetDict, "_id", keepAsObjectFields);
                }
            }
            return (ExpandoObject)targetDict;
        }

        private static readonly Lazy<TimeZoneInfo> SEAsiaTimeZone = new(() =>
        {
            try { return TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"); }
            catch (TimeZoneNotFoundException)
            {
                try { return TimeZoneInfo.FindSystemTimeZoneById("Asia/Ho_Chi_Minh"); }
                catch (TimeZoneNotFoundException) { return TimeZoneInfo.CreateCustomTimeZone("SE Asia Custom", TimeSpan.FromHours(7), "SE Asia Standard Time (Custom)", "SE Asia Standard Time (Custom)"); }
            }
        });

        private static object HandleJsonElement(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    if (element.TryGetDateTime(out var dt)) return dt;
                    return (object?)element.GetString() ?? DBNull.Value;
                case JsonValueKind.Number: return element.GetDecimal();
                case JsonValueKind.True: return true;
                case JsonValueKind.False: return false;
                case JsonValueKind.Null: return DBNull.Value;
                case JsonValueKind.Object:
                    if (element.TryGetProperty("$oid", out var oidElement))
                    {
                        return (object?)oidElement.GetString() ?? DBNull.Value;
                    }
                    if (element.TryGetProperty("$date", out var dateElement))
                    {
                        if (dateElement.ValueKind == JsonValueKind.Object && dateElement.TryGetProperty("$numberLong", out var numberLongElement))
                        {
                            if (numberLongElement.ValueKind == JsonValueKind.String && long.TryParse(numberLongElement.GetString(), out var milliseconds))
                            {
                                return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).UtcDateTime;
                            }
                            else if (numberLongElement.ValueKind == JsonValueKind.Number && numberLongElement.TryGetInt64(out milliseconds))
                            {
                                return DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).UtcDateTime;
                            }
                        }
                        else if (dateElement.TryGetDateTime(out var bsonDate))
                        {
                            return bsonDate;
                        }
                    }
                    return element.GetRawText();
                case JsonValueKind.Array: return element.GetRawText();
                default: return element.ToString();
            }
        }

        private static bool TryGetOidString(object? obj, out string? oidString)
        {
            oidString = null;
            if (obj is IDictionary<string, object?> dict && dict.Count == 1 && dict.TryGetValue("$oid", out var oidValue))
            {
                oidString = oidValue?.ToString();
                return !string.IsNullOrEmpty(oidString);
            }
            return false;
        }

        public static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key, HashSet<string>? keepAsObjectFields = null)
        {
            if (target.ContainsKey(key)) return;
            if (!source.TryGetValue(key, out object? value))
            {
                target[key] = DBNull.Value;
                return;
            }
            if (TryGetOidString(value, out var oidString))
            {
                target[key] = oidString;
                return;
            }
            target[key] = value switch
            {
                null => DBNull.Value,
                DateTime utcDateTime => TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, SEAsiaTimeZone.Value),
                ObjectId oid => oid.ToString(),
                JsonElement jsonElem => HandleJsonElement(jsonElem),
                string str => ConvertStringToTypedValue(str),
                IEnumerable enumerable when value is not string => HandleEnumerable(enumerable, key, keepAsObjectFields),
                _ => value
            };
        }

        /// <summary>
        /// ✅ SỬA ĐỔI: Nâng cấp logic để kiểm tra xem chuỗi có phải là JSON chứa `$oid` hay không.
        /// </summary>
        private static object ConvertStringToTypedValue(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
            {
                return DBNull.Value;
            }

            // --- LOGIC MỚI BẮT ĐẦU TỪ ĐÂY ---
            // Kiểm tra nhanh xem chuỗi có khả năng là một đối tượng JSON không.
            var trimmedStr = str.Trim();
            if (trimmedStr.StartsWith('{') && trimmedStr.EndsWith('}'))
            {
                try
                {
                    // Thử parse chuỗi thành JSON
                    using JsonDocument doc = JsonDocument.Parse(trimmedStr);
                    // Nếu là JSON và có chứa key "$oid", trích xuất giá trị.
                    if (doc.RootElement.TryGetProperty("$oid", out var oidElement))
                    {
                        return oidElement.GetString() ?? (object)DBNull.Value;
                    }
                }
                catch (JsonException)
                {
                    // Nếu không phải JSON hợp lệ, bỏ qua và để logic bên dưới xử lý.
                }
            }

            return str.ToLowerInvariant() switch
            {
                "true" or "1" => true,
                "false" or "0" => false,
                _ => str
            };
        }

        private static object HandleEnumerable(IEnumerable enumerable, string key, HashSet<string>? keepAsObjectFields)
        {
            var firstItem = enumerable.Cast<object>().FirstOrDefault();
            if (firstItem == null && !enumerable.Cast<object>().Any())
            {
                return DBNull.Value;
            }
            if (keepAsObjectFields != null && keepAsObjectFields.Contains(key))
            {
                return enumerable.Cast<object>().ToList();
            }
            return JsonSerializer.Serialize(enumerable);
        }
    }
}