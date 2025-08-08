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
    public class PatientOrdersEtlJob(IConnectionManager sqlConnectionManager, MongoClient mongoClient, INotificationService notificationService) : EtlJob(sqlConnectionManager, mongoClient, notificationService)
    {
        protected override string SourceCollectionName => "patientorders";
        protected override string MongoDatabaseName => "arcusairdb";
        private const string DestPatientOrdersTable = "stg_patientorders";
        private const string DestPatientOrderItemsTable = "stg_patientorderitems";
        private const string DestPatientDiagnosisuidsTable = "stg_patientdiagnosisuids";
        private const string DestDispenseBatchDetailTable = "stg_dispensebatchdetail";

        protected override List<string> StagingTables =>
        [
            "stg_patientorders",
            "stg_patientorderitems",
            "stg_patientdiagnosisuids",
            "stg_dispensebatchdetail"
        ];

        public new async Task RunAsync(PerformContext? context, JobSettings jobSettings)
        {
            context?.WriteLine("Starting job execution for PatientOrders...");
            await base.RunAsync(context, jobSettings);
            context?.WriteLine("Job execution for PatientOrders completed.");
        }

        protected override EtlPipeline BuildPipeline(List<ExpandoObject> batchData, PerformContext? context)
        {
            var patientordersDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrdersTable);
            var patientorderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrderItemsTable);
            var patientdiagnosisuidsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var dispensebatchdetailDef = TableDefinition.FromTableName(SqlConnectionManager, DestDispenseBatchDetailTable);

            var source = new MemorySource<ExpandoObject>(batchData);
            var logErrors = CreateErrorLoggingDestination(context);

            var multicastOrders = new Multicast<ExpandoObject>();
            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: stg_patientorders (Không đổi) ==================
            var transformAndMapOrders = CreateTransformAndMapComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var destPatientOrders = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrdersTable);
            multicastOrders.LinkTo(transformAndMapOrders);
            transformAndMapOrders.LinkTo(destPatientOrders);
            transformAndMapOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            // ================== FLOW 2: patientorderitems và dispensebatchdetail (ĐÃ TỐI ƯU HÓA) ==================

            // Bước 2.1: Vừa làm phẳng, vừa biến đổi `patientorderitems` trong MỘT BƯỚC.
            // Component này sẽ lấy `patientorder` làm đầu vào, tạo ra các `patientorderitem` đã được biến đổi.
            // Quan trọng: Chúng ta giữ lại mảng `dispensebatchdetail` để có thể xử lý ở bước sau.
            var flattenAndTransformOrderItems = CreateFlattenAndTransformComponent(
                arrayFieldName: "patientorderitems",
                foreignKeyName: "patientordersuid",
                targetColumns: [.. patientorderitemsDef.Columns.Select(c => c.Name)],
                keepAsObjectFields: ["dispensebatchdetail"] // Giữ lại mảng con để xử lý tiếp
            );
            multicastOrders.LinkTo(flattenAndTransformOrderItems, o => ((IDictionary<string, object?>)o).ContainsKey("patientorderitems"));
            flattenAndTransformOrderItems.LinkErrorTo(logErrors);

            // Bước 2.2: Dữ liệu đầu ra của bước 2.1 là các `patientorderitem` đã được xử lý.
            // Giờ chúng ta cần một Multicast mới để gửi chúng đến hai đích khác nhau.
            var multicastProcessedItems = new Multicast<ExpandoObject>();
            flattenAndTransformOrderItems.LinkTo(multicastProcessedItems);

            // -- Nhánh 2.2.1: Ghi các `patientorderitem` đã xử lý vào bảng đích.
            // Không cần biến đổi thêm nữa, chỉ cần gửi đến đích.
            var destPatientOrderItems = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrderItemsTable);
            multicastProcessedItems.LinkTo(destPatientOrderItems);
            destPatientOrderItems.LinkErrorTo(logErrors);

            // -- Nhánh 2.2.2: Từ các `patientorderitem` đã xử lý, làm phẳng và ghi `dispensebatchdetail`.
            var flattenAndTransformDispense = CreateFlattenAndTransformComponent(
                arrayFieldName: "dispensebatchdetail",
                foreignKeyName: "patientorderitemsuid", // Khóa ngoại là ID của item
                targetColumns: [.. dispensebatchdetailDef.Columns.Select(c => c.Name)],
                parentIdFieldName: "_id" // ID của item cha là "_id"
            );
            var destDispenseBatchDetail = new DbDestination<ExpandoObject>(SqlConnectionManager, DestDispenseBatchDetailTable);

            multicastProcessedItems.LinkTo(flattenAndTransformDispense, item => ((IDictionary<string, object?>)item).ContainsKey("dispensebatchdetail"));
            flattenAndTransformDispense.LinkTo(destDispenseBatchDetail);
            flattenAndTransformDispense.LinkErrorTo(logErrors);
            destDispenseBatchDetail.LinkErrorTo(logErrors);

            // ================== FLOW 3: stg_patientdiagnosisuids (Không đổi) ==================
            var flattenAndTransformDiagnosis = CreateFlattenAndTransformComponent(
                arrayFieldName: "patientdiagnosisuids",
                foreignKeyName: "patientordersuid",
                targetColumns: [.. patientdiagnosisuidsDef.Columns.Select(c => c.Name)]
            );
            var destPatientDiagnosisUids = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            multicastOrders.LinkTo(flattenAndTransformDiagnosis, o => ((IDictionary<string, object?>)o).ContainsKey("patientdiagnosisuids"));
            flattenAndTransformDiagnosis.LinkTo(destPatientDiagnosisUids);
            flattenAndTransformDiagnosis.LinkErrorTo(logErrors);
            destPatientDiagnosisUids.LinkErrorTo(logErrors);

            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destPatientDiagnosisUids, destDispenseBatchDetail],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: "sp_MergePatientOrdersData"
            );
        }
    }
}