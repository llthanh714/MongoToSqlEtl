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
            IDataFlowSource<ExpandoObject> parentSource,
            CustomDestination<ETLBoxError> errorDestination,
            List<IDataFlowDestination<ExpandoObject>> destinations,
            PerformContext? context)
        {
            context?.WriteLine($"Processing mapping for: {currentMapping.MongoFieldName} -> {currentMapping.SqlTableName}");

            var tableDef = TableDefinition.FromTableName(SqlConnectionManager, currentMapping.SqlTableName);
            var transform = CreateTransformAndMapComponent([.. tableDef.Columns.Select(c => c.Name)]);

            var destination = new DbMerge<ExpandoObject>(SqlConnectionManager, currentMapping.SqlTableName)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "id" }]
            };

            destinations.Add(destination);

            var multicast = new Multicast<ExpandoObject>();
            parentSource.LinkTo(multicast);

            multicast.LinkTo(transform);
            transform.LinkTo(destination);
            transform.LinkErrorTo(errorDestination);
            destination.LinkErrorTo(errorDestination);

            var childMappings = _jobSettings.Mappings.Where(m => m.ParentMongoFieldName == currentMapping.MongoFieldName).ToList();

            foreach (var childMapping in childMappings)
            {
                var flattenAndTransform = CreateFlattenAndTransformComponent(
                    arrayFieldName: childMapping.MongoFieldName,
                    foreignKeyName: childMapping.ForeignKeyName,
                    targetColumns: [.. TableDefinition.FromTableName(SqlConnectionManager, childMapping.SqlTableName).Columns.Select(c => c.Name)],
                    parentIdFieldName: "_id"
                );

                multicast.LinkTo(flattenAndTransform,
                    obj => ((IDictionary<string, object?>)obj).ContainsKey(childMapping.MongoFieldName));
                flattenAndTransform.LinkErrorTo(errorDestination);

                ProcessMappings(childMapping, flattenAndTransform, errorDestination, destinations, context);
            }
        }
    }
}