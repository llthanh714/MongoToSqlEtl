using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Server;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Jobs;
using MongoToSqlEtl.Services;
using System.Dynamic;

namespace MongoToSqlEtl.StoredJobs
{
    /// <summary>
    /// Lớp triển khai cụ thể cho việc ETL collection 'orderitems'.
    /// </summary>
    public class OrderItemsEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        protected override string SourceCollectionName => "orderitems";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestOrderItemsTable = "orderitems";
        private const string DestOrderItemCodesTable = "orderitemcodes";
        private const string DestOrderItemInstructionsTable = "orderiteminstructions";

        protected override List<string> StagingTables => [];

        public new Task RunAsync(PerformContext? context, JobSettings jobSettings)
        {
            return base.RunAsync(context, jobSettings);
        }

        protected override void SetJobSettings(JobSettings jobSettings)
        {
            // Không cần làm gì ở đây cho các job có logic cố định.
        }

        protected override EtlPipeline BuildPipeline(List<ExpandoObject> batchData, PerformContext? context)
        {
            // 1. Định nghĩa các bảng
            var orderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestOrderItemsTable);
            var orderitemcodesDef = TableDefinition.FromTableName(SqlConnectionManager, DestOrderItemCodesTable);
            var orderiteminstructionsDef = TableDefinition.FromTableName(SqlConnectionManager, DestOrderItemInstructionsTable);

            // 2. Các component nguồn và lỗi
            var source = new MemorySource<ExpandoObject>(batchData);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. Multicast cấp 1 cho 'orderitems'
            var multicast = new Multicast<ExpandoObject>();
            source.LinkTo(multicast);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: orderitems ==================
            var transformAndMapOrderItems = CreateTransformAndMapComponent([.. orderitemsDef.Columns.Select(c => c.Name)]);

            var destOrderItems = new DbMerge<ExpandoObject>(SqlConnectionManager, DestOrderItemsTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };

            multicast.LinkTo(transformAndMapOrderItems);
            transformAndMapOrderItems.LinkTo(destOrderItems);
            transformAndMapOrderItems.LinkErrorTo(logErrors);
            destOrderItems.LinkErrorTo(logErrors);

            // ================== FLOW 2: orderitemcodes ==================
            var flattenAndTransformCodes = CreateFlattenAndTransformComponent(
                arrayFieldName: "orderitemcodes",
                foreignKeyName: "orderitemsuid",
                targetColumns: [.. orderitemcodesDef.Columns.Select(c => c.Name)]
            );

            var destCodes = new DbMerge<ExpandoObject>(SqlConnectionManager, DestOrderItemCodesTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };

            multicast.LinkTo(flattenAndTransformCodes, o => ((IDictionary<string, object?>)o).ContainsKey("orderitemcodes"));
            flattenAndTransformCodes.LinkTo(destCodes);
            flattenAndTransformCodes.LinkErrorTo(logErrors);
            destCodes.LinkErrorTo(logErrors);

            // ================== FLOW 3: orderiteminstructions ==================
            var flattenAndTransformInstructions = CreateFlattenAndTransformComponent(
                arrayFieldName: "orderiteminstructions",
                foreignKeyName: "orderitemsuid",
                targetColumns: [.. orderiteminstructionsDef.Columns.Select(c => c.Name)]
            );

            var destInstructions = new DbMerge<ExpandoObject>(SqlConnectionManager, DestOrderItemInstructionsTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };

            multicast.LinkTo(flattenAndTransformInstructions, o => ((IDictionary<string, object?>)o).ContainsKey("orderiteminstructions"));
            flattenAndTransformInstructions.LinkTo(destInstructions);
            flattenAndTransformInstructions.LinkErrorTo(logErrors);
            destInstructions.LinkErrorTo(logErrors);


            // 4. Trả về pipeline
            return new EtlPipeline(
                Source: source,
                Destinations: [destOrderItems, destCodes, destInstructions],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: string.Empty
            );
        }
    }
}