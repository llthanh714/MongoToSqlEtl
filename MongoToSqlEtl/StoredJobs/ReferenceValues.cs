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
    /// Lớp triển khai cụ thể cho việc ETL collection 'referencevalues'.
    /// </summary>
    public class ReferenceValues(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        protected override string SourceCollectionName => "referencevalues";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestReferenceValuesTable = "referencevalues";

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
            var referencevaluesDef = TableDefinition.FromTableName(SqlConnectionManager, DestReferenceValuesTable);

            // 2. Các component nguồn và lỗi
            var source = new MemorySource<ExpandoObject>(batchData);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. etl cho referencevalues
            var transformAndMapObjects = CreateTransformAndMapComponent([.. referencevaluesDef.Columns.Select(c => c.Name)]);

            var destObjects = new DbMerge<ExpandoObject>(SqlConnectionManager, DestReferenceValuesTable)
            {
                MergeMode = MergeMode.Delta,
                // SỬA ĐỔI: Sử dụng "id" làm khóa chính
                IdColumns = [new IdColumn { IdPropertyName = "id" }]
            };

            source.LinkTo(transformAndMapObjects);
            transformAndMapObjects.LinkTo(destObjects);
            transformAndMapObjects.LinkErrorTo(logErrors);
            destObjects.LinkErrorTo(logErrors);

            // 4. Trả về pipeline
            return new EtlPipeline(
                Source: source,
                Destinations: [destObjects],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: string.Empty // Không sử dụng stored procedure
            );
        }
    }
}