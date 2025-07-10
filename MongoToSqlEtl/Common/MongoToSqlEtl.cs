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
                MergeMode = MergeMode.InsertsAndUpdates,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }],
                BatchSize = 500
            };
        }

        public static RowTransformation<ExpandoObject> CreateTransformComponent(HashSet<string> targetColumns)
        {
            return new RowTransformation<ExpandoObject>(row => TransformObject(row, targetColumns));
        }

        public static ExpandoObject TransformObject(ExpandoObject sourceRow, ICollection<string> targetColumns)
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

        public static void MapProperty(IDictionary<string, object?> source, IDictionary<string, object?> target, string key)
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
    }
}
