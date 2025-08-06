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
    /// Lớp triển khai cụ thể cho việc ETL collection 'patients'.
    /// </summary>
    public class PatientsEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        protected override string SourceCollectionName => "patients";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestPatientTable = "patients";
        private const string DestPatientAddressTable = "patientsaddress";
        private const string DestPatientContactTable = "patientscontact";

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
            var patientsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientTable);
            var patientsaddressDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientAddressTable);
            var patientscontactDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientContactTable);

            // 2. Các component nguồn và lỗi
            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. Multicast cấp 1 cho 'patients'
            var multicastPatients = new Multicast<ExpandoObject>();
            source.LinkTo(multicastPatients);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: patients ==================
            var transformAndMapPatients = CreateTransformAndMapComponent([.. patientsDef.Columns.Select(c => c.Name)]);
            // var destPatients = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientTable);

            var destPatients = new DbMerge<ExpandoObject>(SqlConnectionManager, DestPatientTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };

            multicastPatients.LinkTo(transformAndMapPatients);
            transformAndMapPatients.LinkTo(destPatients);
            transformAndMapPatients.LinkErrorTo(logErrors);
            destPatients.LinkErrorTo(logErrors);

            // ================== FLOW 2: patientsaddress ==================
            var flattenAndTransformPatientsAddress = CreateFlattenAndTransformComponent(
                arrayFieldName: "address",
                foreignKeyName: "patientsuid",
                targetColumns: [.. patientsaddressDef.Columns.Select(c => c.Name)]
            );
            
            // var destPatientsAddress = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientAddressTable);

            var destPatientsAddress = new DbMerge<ExpandoObject>(SqlConnectionManager, DestPatientAddressTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };

            multicastPatients.LinkTo(flattenAndTransformPatientsAddress, o => ((IDictionary<string, object?>)o).ContainsKey("address"));
            flattenAndTransformPatientsAddress.LinkTo(destPatientsAddress);
            flattenAndTransformPatientsAddress.LinkErrorTo(logErrors);
            destPatientsAddress.LinkErrorTo(logErrors);

            // ================== FLOW 3: patientscontact ==================
            var flattenAndTransformPatientsContact = CreateFlattenAndTransformComponent(
                arrayFieldName: "contact",
                foreignKeyName: "patientsuid",
                targetColumns: [.. patientscontactDef.Columns.Select(c => c.Name)]
            );
            
            // var destPatientsContact = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientContactTable);

            var destPatientsContact = new DbMerge<ExpandoObject>(SqlConnectionManager, DestPatientContactTable)
            {
                MergeMode = MergeMode.Delta,
                IdColumns = [new IdColumn { IdPropertyName = "_id" }]
            };

            multicastPatients.LinkTo(flattenAndTransformPatientsContact, o => ((IDictionary<string, object?>)o).ContainsKey("contact"));
            flattenAndTransformPatientsContact.LinkTo(destPatientsContact);
            flattenAndTransformPatientsContact.LinkErrorTo(logErrors);
            destPatientsContact.LinkErrorTo(logErrors);


            // 4. Trả về pipeline
            return new EtlPipeline(
                Source: source,
                Destinations: [destPatients, destPatientsAddress, destPatientsContact],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: string.Empty // Không sử dụng stored procedure
            );
        }
    }
}