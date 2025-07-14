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
        // Object lock tĩnh để đảm bảo an toàn luồng khi tạo ExpandoObjects
        private static readonly object ExpandoLock = new();

        public static DbMerge<ExpandoObject> CreateDbMergeDestination(IConnectionManager conn, string tableName)
        {
            return new DbMerge<ExpandoObject>(conn, tableName)
            {
                MergeMode = MergeMode.InsertsAndUpdates,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }],
                BatchSize = 500
            };
        }

        public static RowTransformation<ExpandoObject> CreateTransformComponent(HashSet<string> targetColumns, HashSet<string>? keepAsObjectFields = null)
        {
            return new RowTransformation<ExpandoObject>(row => TransformObject(row, targetColumns, keepAsObjectFields));
        }

        public static ExpandoObject TransformObject(ExpandoObject sourceRow, ICollection<string> targetColumns, HashSet<string>? keepAsObjectFields = null)
        {
            lock (ExpandoLock)
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
        }

        public static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key, HashSet<string>? keepAsObjectFields = null)
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
                else if (value is IEnumerable enumerable && value is not string)
                {
                    bool isEmpty = enumerable is not ICollection collection
                        ? !enumerable.Cast<object>().Any()
                        : collection.Count == 0;

                    if (isEmpty)
                    {
                        target[key] = DBNull.Value;
                    }
                    else
                    {
                        if (keepAsObjectFields != null && keepAsObjectFields.Contains(key))
                        {
                            target[key] = value;
                        }
                        else
                        {
                            target[key] = JsonSerializer.Serialize(value);
                        }
                    }
                }
                else { target[key] = value; }
            }
            else { target[key] = DBNull.Value; }
        }
    }
}
