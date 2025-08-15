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
    /// Lớp triển khai cụ thể cho việc ETL collection 'patients'.
    /// </summary>
    public class PatientsEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        // Các thuộc tính này không thay đổi
        protected override string SourceCollectionName => "patients";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestPatientTable = "patients";
        private const string DestPatientAddressTable = "patientsaddress";
        private const string DestPatientContactTable = "patientscontact";
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
            var patientsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientTable);
            var patientsaddressDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientAddressTable);
            var patientscontactDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientContactTable);

            // 2. Các component nguồn và lỗi
            var source = new MemorySource<ExpandoObject>(batchData);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. Multicast cấp 1 cho 'patients'
            var multicastPatients = new Multicast<ExpandoObject>();
            source.LinkTo(multicastPatients);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: patients ==================
            var transformAndMapPatients = CreateTransformAndMapComponent([.. patientsDef.Columns.Select(c => c.Name)]);
            var destPatients = new DbMerge<ExpandoObject>(SqlConnectionManager, DestPatientTable)
            {
                MergeMode = MergeMode.Delta,
                // SỬA ĐỔI: Sử dụng "id" làm khóa chính
                IdColumns = [new IdColumn { IdPropertyName = "id" }]
            };
            multicastPatients.LinkTo(transformAndMapPatients);
            transformAndMapPatients.LinkTo(destPatients);
            transformAndMapPatients.LinkErrorTo(logErrors);
            destPatients.LinkErrorTo(logErrors);

            // ================== FLOW 2: patientsaddress ==================
            var flattenAndTransformPatientsAddress = CreateFlattenAndTransformComponent(
                arrayFieldName: "address",
                foreignKeyName: "patientsuid",
                targetColumns: [.. patientsaddressDef.Columns.Select(c => c.Name)],
                parentIdFieldName: "id" // Sử dụng "id" của cha
            );
            var destPatientsAddress = new DbMerge<ExpandoObject>(SqlConnectionManager, DestPatientAddressTable)
            {
                MergeMode = MergeMode.Delta,
                // SỬA ĐỔI: Sử dụng "id" làm khóa chính
                IdColumns = [new IdColumn { IdPropertyName = "id" }]
            };
            multicastPatients.LinkTo(flattenAndTransformPatientsAddress, o => ((IDictionary<string, object?>)o).ContainsKey("address"));
            flattenAndTransformPatientsAddress.LinkTo(destPatientsAddress);
            flattenAndTransformPatientsAddress.LinkErrorTo(logErrors);
            destPatientsAddress.LinkErrorTo(logErrors);

            // ================== FLOW 3: patientscontact ==================
            var flattenAndTransformPatientsContact = CreateFlattenAndTransformComponent(
                arrayFieldName: "contact",
                foreignKeyName: "patientsuid",
                targetColumns: [.. patientscontactDef.Columns.Select(c => c.Name)],
                parentIdFieldName: "id" // Sử dụng "id" của cha
            );
            var destPatientsContact = new DbMerge<ExpandoObject>(SqlConnectionManager, DestPatientContactTable)
            {
                MergeMode = MergeMode.Delta,
                // SỬA ĐỔI: Sử dụng "id" làm khóa chính
                IdColumns = [new IdColumn { IdPropertyName = "id" }]
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
                SqlStoredProcedureName: string.Empty
            );
        }
    }
}