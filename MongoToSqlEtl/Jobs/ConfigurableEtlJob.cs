using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Console;
using Hangfire.Server;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Services;
using System.Dynamic;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// A flexible ETL job that is fully configurable from JobSettings.
    /// It automatically builds the pipeline based on the defined mappings.
    /// </summary>
    public class ConfigurableEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService)
        : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        private JobSettings _jobSettings = new();

        protected override string SourceCollectionName =>
            _jobSettings.Mappings.FirstOrDefault(m => string.IsNullOrEmpty(m.ParentMongoFieldName))?.MongoFieldName ??
            throw new InvalidOperationException("Mapping for the root collection was not found (ParentMongoFieldName is null).");

        protected override string MongoDatabaseName => "arcusairdb";

        // Trả về một danh sách rỗng để ngăn việc gọi TruncateStagingTablesAsync.
        protected override List<string> StagingTables => [];

        public new Task RunAsync(PerformContext? context, JobSettings jobSettings)
        {
            return base.RunAsync(context, jobSettings);
        }

        protected override void SetJobSettings(JobSettings jobSettings)
        {
            _jobSettings = jobSettings;
        }

        protected override EtlPipeline BuildPipeline(List<ExpandoObject> batchData, PerformContext? context)
        {
            var source = new MemorySource<ExpandoObject>(batchData);
            var logErrors = CreateErrorLoggingDestination(context);
            var destinations = new List<IDataFlowDestination<ExpandoObject>>();

            var rootMapping = _jobSettings.Mappings.FirstOrDefault(m => string.IsNullOrEmpty(m.ParentMongoFieldName))
                ?? throw new InvalidOperationException("Job configuration is missing the root mapping (ParentMongoFieldName is null or empty).");

            var rootMulticast = new Multicast<ExpandoObject>();
            source.LinkTo(rootMulticast);
            source.LinkErrorTo(logErrors);

            ProcessMappings(rootMapping, rootMulticast, logErrors, destinations, context);

            return new EtlPipeline(
                Source: source,
                Destinations: destinations,
                ErrorDestination: logErrors,
                SqlStoredProcedureName: _jobSettings.MergeStoredProcedure ?? string.Empty
            );
        }

        private void ProcessMappings(
            TableMapping currentMapping,
            IDataFlowSource<ExpandoObject> sourceOfRawDataForThisLevel,
            CustomDestination<ETLBoxError> errorDestination,
            List<IDataFlowDestination<ExpandoObject>> destinations,
            PerformContext? context)
        {
            context?.WriteLine($"Processing mapping for: {currentMapping.MongoFieldName} -> {currentMapping.SqlTableName}");

            // 1. Tạo một điểm rẽ nhánh (Multicast) cho luồng dữ liệu GỐC của cấp hiện tại.
            // Điều này cho phép chúng ta vừa xử lý-lưu cấp hiện tại, vừa làm phẳng-truyền cho cấp con.
            var multicastRaw = new Multicast<ExpandoObject>();
            sourceOfRawDataForThisLevel.LinkTo(multicastRaw);

            // 2. NHÁNH A: Xử lý và lưu dữ liệu cho bảng của cấp hiện tại.
            var tableDef = TableDefinition.FromTableName(SqlConnectionManager, currentMapping.SqlTableName);
            var transform = CreateTransformAndMapComponent([.. tableDef.Columns.Select(c => c.Name)]);
            var destination = new DbMerge<ExpandoObject>(SqlConnectionManager, currentMapping.SqlTableName)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "id" }]
            };
            destinations.Add(destination);

            multicastRaw.LinkTo(transform);
            transform.LinkTo(destination);
            transform.LinkErrorTo(errorDestination);
            destination.LinkErrorTo(errorDestination);

            // 3. NHÁNH B: Tìm và xử lý tất cả các mapping con.
            var childMappings = _jobSettings.Mappings.Where(m => m.ParentMongoFieldName == currentMapping.MongoFieldName).ToList();
            foreach (var childMapping in childMappings)
            {
                // 3.1. Đối với mỗi con, tạo một component "làm phẳng".
                // Component này lấy dữ liệu từ multicast GỐC ở trên.
                var flatten = CreateFlattenComponent(
                    arrayFieldName: childMapping.MongoFieldName,
                    foreignKeyName: childMapping.ForeignKeyName,
                    // QUAN TRỌNG: Vì chúng ta đang làm việc trên dữ liệu gốc,
                    // trường ID của cha luôn là "_id" tại thời điểm này.
                    parentIdFieldName: "_id"
                );
                multicastRaw.LinkTo(flatten, obj => ((IDictionary<string, object?>)obj).ContainsKey(childMapping.MongoFieldName));
                flatten.LinkErrorTo(errorDestination);

                // 3.2. Luồng đầu ra của component 'flatten' chính là luồng dữ liệu GỐC cho cấp con.
                // Gọi đệ quy để lặp lại quy trình này cho cấp con.
                ProcessMappings(childMapping, flatten, errorDestination, destinations, context);
            }
        }
    }
}