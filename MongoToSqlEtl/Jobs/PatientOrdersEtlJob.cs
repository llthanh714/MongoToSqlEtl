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
    /// Lớp triển khai cụ thể cho việc ETL collection 'patientorders'.
    /// </summary>
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

        public new async Task RunAsync(PerformContext? context)
        {
            context?.WriteLine("Starting job execution...");
            await base.RunAsync(context);
            context?.WriteLine("Job execution completed.");
        }

        protected override EtlPipeline BuildPipeline(DateTime startDate, DateTime endDate, List<string> failedIds, PerformContext? context)
        {
            // 1. Định nghĩa các bảng (Không đổi)
            var patientordersDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrdersTable);
            var patientorderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrderItemsTable);
            var patientdiagnosisuidsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var dispensebatchdetailDef = TableDefinition.FromTableName(SqlConnectionManager, DestDispenseBatchDetailTable);

            // 2. Các component nguồn và lỗi (Không đổi)
            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. Multicast cấp 1 (Không đổi)
            var multicastOrders = new Multicast<ExpandoObject>();
            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: stg_patientorders (Chỉ dùng Transform & Map) ==================
            var transformAndMapOrders = CreateTransformAndMapComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var destPatientOrders = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrdersTable);

            multicastOrders.LinkTo(transformAndMapOrders);
            transformAndMapOrders.LinkTo(destPatientOrders);
            transformAndMapOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            // ================== FLOW 2: patientorderitems và các con ==================
            // Bước 2.1: Chỉ làm phẳng `patientorderitems`
            var flattenOrderItems = CreateFlattenComponent("patientorderitems", "patientordersuid");
            multicastOrders.LinkTo(flattenOrderItems, o => ((IDictionary<string, object?>)o).ContainsKey("patientorderitems"));
            flattenOrderItems.LinkErrorTo(logErrors);

            // Bước 2.2: Multicast cấp 2 cho các item đã được làm phẳng
            var multicastItems = new Multicast<ExpandoObject>();
            flattenOrderItems.LinkTo(multicastItems);

            // -- Nhánh 2.3: Ghi vào `stg_patientorderitems` --
            var transformAndMapItems = CreateTransformAndMapComponent(
                [.. patientorderitemsDef.Columns.Select(c => c.Name)],
                // Yêu cầu giữ lại `dispensebatchdetail` để nhánh dưới có thể xử lý
                ["dispensebatchdetail"]
            );
            var destPatientOrderItems = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrderItemsTable);

            multicastItems.LinkTo(transformAndMapItems);
            transformAndMapItems.LinkTo(destPatientOrderItems);
            destPatientOrderItems.LinkErrorTo(logErrors); // Lỗi sau khi transform sẽ được bắt ở đây

            // -- Nhánh 2.4: Ghi vào `stg_dispensebatchdetail` --
            // Bước 2.4.1: Làm phẳng cấp 2
            var flattenDispenseBatch = CreateFlattenComponent(
                "dispensebatchdetail",
                "patientorderitemsuid"
            );
            multicastItems.LinkTo(flattenDispenseBatch, item => ((IDictionary<string, object?>)item).ContainsKey("dispensebatchdetail"));
            flattenDispenseBatch.LinkErrorTo(logErrors);

            // Bước 2.4.2: Transform và Map cho dispensebatchdetail
            var transformAndMapDispenseBatch = CreateTransformAndMapComponent(
                [.. dispensebatchdetailDef.Columns.Select(c => c.Name)]
            );
            var destDispenseBatchDetail = new DbDestination<ExpandoObject>(SqlConnectionManager, "stg_dispensebatchdetail");

            flattenDispenseBatch.LinkTo(transformAndMapDispenseBatch);
            transformAndMapDispenseBatch.LinkTo(destDispenseBatchDetail);
            destDispenseBatchDetail.LinkErrorTo(logErrors);


            // ================== FLOW 3: stg_patientdiagnosisuids ==================
            var flattenDiagnosisUids = CreateFlattenComponent("patientdiagnosisuids", "patientordersuid");
            multicastOrders.LinkTo(flattenDiagnosisUids, o => ((IDictionary<string, object?>)o).ContainsKey("patientdiagnosisuids"));
            flattenDiagnosisUids.LinkErrorTo(logErrors);

            var transformAndMapDiagnosis = CreateTransformAndMapComponent([.. patientdiagnosisuidsDef.Columns.Select(c => c.Name)]);
            var destPatientDiagnosisUids = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientDiagnosisuidsTable);

            flattenDiagnosisUids.LinkTo(transformAndMapDiagnosis);
            transformAndMapDiagnosis.LinkTo(destPatientDiagnosisUids);
            destPatientDiagnosisUids.LinkErrorTo(logErrors);

            // 4. Trả về pipeline (Không đổi)
            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destPatientDiagnosisUids, destDispenseBatchDetail],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: "sp_MergePatientOrdersData"
            );
        }
    }
}