using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Console;
using Hangfire.Server;
using MongoDB.Driver;
using MongoToSqlEtl.Services;
using System.Dynamic;

namespace MongoToSqlEtl.Jobs
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

        public new async Task RunAsync(PerformContext? context, int maxBatchIntervalInMinutes)
        {
            context?.WriteLine("Starting job execution...");
            await base.RunAsync(context, maxBatchIntervalInMinutes);
            context?.WriteLine("Job execution completed.");
        }

        protected override EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds, PerformContext? context)
        {
            // 1. Định nghĩa các bảng
            var referencevaluesDef = TableDefinition.FromTableName(SqlConnectionManager, DestReferenceValuesTable);

            // 2. Các component nguồn và lỗi
            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. etl cho referencevalues
            var transformAndMapObjects = CreateTransformAndMapComponent([.. referencevaluesDef.Columns.Select(c => c.Name)]);

            var destObjects = new DbMerge<ExpandoObject>(SqlConnectionManager, DestReferenceValuesTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
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