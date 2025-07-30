using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Console;
using Hangfire.Server;
using MongoDB.Driver;
using MongoToSqlEtl.Jobs.Services;
using System.Dynamic;

namespace MongoToSqlEtl.Jobs.Jobs
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
            // 1. Định nghĩa các bảng
            var patientordersDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrdersTable);
            var patientorderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrderItemsTable);
            var patientdiagnosisuidsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var dispensebatchdetailDef = TableDefinition.FromTableName(SqlConnectionManager, DestDispenseBatchDetailTable);

            // 2. Các component nguồn và lỗi
            var source = CreateMongoDbSource(startDate, endDate, failedIds);
            var logErrors = CreateErrorLoggingDestination(context);

            // 3. Multicast cấp 1 cho 'patientorders'
            var multicastOrders = new Multicast<ExpandoObject>();
            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: stg_patientorders ==================
            var transformAndMapOrders = CreateTransformAndMapComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var destPatientOrders = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrdersTable);

            multicastOrders.LinkTo(transformAndMapOrders);
            transformAndMapOrders.LinkTo(destPatientOrders);
            transformAndMapOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);

            // ================== FLOW 2: patientorderitems và dispensebatchdetail ==================

            // Bước 2.1: Làm phẳng `patientorderitems` từ `patientorders`
            var flattenOrderItems = CreateFlattenComponent("patientorderitems", "patientordersuid");
            multicastOrders.LinkTo(flattenOrderItems, o => ((IDictionary<string, object?>)o).ContainsKey("patientorderitems"));
            flattenOrderItems.LinkErrorTo(logErrors);

            // Bước 2.2: Multicast cấp 2 để xử lý các `patientorderitem` riêng lẻ
            var multicastItems = new Multicast<ExpandoObject>();
            flattenOrderItems.LinkTo(multicastItems);

            // -- Nhánh 2.3: Ghi vào `stg_patientorderitems` --
            var transformAndMapItems = CreateTransformAndMapComponent(
                targetColumns: [.. patientorderitemsDef.Columns.Select(c => c.Name)],
                // ✨ FIX: Yêu cầu giữ lại mảng `dispensebatchdetail` để nhánh 2.4 có thể xử lý.
                keepAsObjectFields: ["dispensebatchdetail"]
            );
            var destPatientOrderItems = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrderItemsTable);

            multicastItems.LinkTo(transformAndMapItems);
            transformAndMapItems.LinkTo(destPatientOrderItems);
            transformAndMapItems.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            // -- Nhánh 2.4: Ghi vào `stg_dispensebatchdetail` --
            var flattenAndTransformDispense = CreateFlattenAndTransformComponent(
                arrayFieldName: "dispensebatchdetail",
                foreignKeyName: "patientorderitemsuid", // Khóa ngoại trong bảng dispense
                targetColumns: [.. dispensebatchdetailDef.Columns.Select(c => c.Name)],
                parentIdFieldName: "_id"
            );
            var destDispenseBatchDetail = new DbDestination<ExpandoObject>(SqlConnectionManager, DestDispenseBatchDetailTable);

            multicastItems.LinkTo(flattenAndTransformDispense, item => ((IDictionary<string, object?>)item).ContainsKey("dispensebatchdetail"));
            flattenAndTransformDispense.LinkTo(destDispenseBatchDetail);
            flattenAndTransformDispense.LinkErrorTo(logErrors);
            destDispenseBatchDetail.LinkErrorTo(logErrors);

            // ================== FLOW 3: stg_patientdiagnosisuids ==================
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


            // 4. Trả về pipeline
            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destPatientDiagnosisUids, destDispenseBatchDetail],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: "sp_MergePatientOrdersData"
            );
        }
    }
}