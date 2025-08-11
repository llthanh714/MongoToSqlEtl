using ETLBox;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using Hangfire.Server;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
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
            // 1. Định nghĩa các bảng và component chung
            var patientordersDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrdersTable);
            var patientorderitemsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientOrderItemsTable);
            var patientdiagnosisuidsDef = TableDefinition.FromTableName(SqlConnectionManager, DestPatientDiagnosisuidsTable);
            var dispensebatchdetailDef = TableDefinition.FromTableName(SqlConnectionManager, DestDispenseBatchDetailTable);

            var source = new MemorySource<ExpandoObject>(batchData);
            var logErrors = CreateErrorLoggingDestination(context);

            // Multicast chính để phân phối các document 'patientorder'
            var multicastOrders = new Multicast<ExpandoObject>();
            source.LinkTo(multicastOrders);
            source.LinkErrorTo(logErrors);

            // ================== FLOW 1: stg_patientorders ==================
            // Luồng này hoạt động đúng: biến đổi và ghi dữ liệu cho bảng cha.
            var transformAndMapOrders = CreateTransformAndMapComponent([.. patientordersDef.Columns.Select(c => c.Name)]);
            var destPatientOrders = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrdersTable);

            multicastOrders.LinkTo(transformAndMapOrders);
            transformAndMapOrders.LinkTo(destPatientOrders);
            transformAndMapOrders.LinkErrorTo(logErrors);
            destPatientOrders.LinkErrorTo(logErrors);


            // ================== FLOW 2: patientorderitems và dispensebatchdetail ==================

            // Bước 2.1: CHỈ làm phẳng (flatten) mảng 'patientorderitems'.
            var flattenOrderItems = CreateFlattenComponent(
                arrayFieldName: "patientorderitems",
                foreignKeyName: "patientordersuid"
            );
            multicastOrders.LinkTo(flattenOrderItems, order => ((IDictionary<string, object?>)order).ContainsKey("patientorderitems"));
            flattenOrderItems.LinkErrorTo(logErrors);

            // Bước 2.2: Multicast các 'patientorderitem' thô (chưa bị biến đổi).
            var multicastRawItems = new Multicast<ExpandoObject>();
            flattenOrderItems.LinkTo(multicastRawItems);

            // -- Nhánh 2.2.1: Biến đổi và ghi vào stg_patientorderitems.
            var transformItems = CreateTransformAndMapComponent([.. patientorderitemsDef.Columns.Select(c => c.Name)]);
            var destPatientOrderItems = new DbDestination<ExpandoObject>(SqlConnectionManager, DestPatientOrderItemsTable);
            multicastRawItems.LinkTo(transformItems);
            transformItems.LinkTo(destPatientOrderItems);
            transformItems.LinkErrorTo(logErrors);
            destPatientOrderItems.LinkErrorTo(logErrors);

            // -- Nhánh 2.2.2: Từ các item thô, làm phẳng và ghi 'dispensebatchdetail'.
            var flattenAndTransformDispense = CreateFlattenAndTransformComponent(
                arrayFieldName: "dispensebatchdetail",
                foreignKeyName: "patientorderitemsuid",
                targetColumns: [.. dispensebatchdetailDef.Columns.Select(c => c.Name)],
                parentIdFieldName: "_id"
            );
            var destDispenseBatchDetail = new DbDestination<ExpandoObject>(SqlConnectionManager, DestDispenseBatchDetailTable);
            multicastRawItems.LinkTo(flattenAndTransformDispense, item => ((IDictionary<string, object?>)item).ContainsKey("dispensebatchdetail"));
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
            multicastOrders.LinkTo(flattenAndTransformDiagnosis, order => ((IDictionary<string, object?>)order).ContainsKey("patientdiagnosisuids"));
            flattenAndTransformDiagnosis.LinkTo(destPatientDiagnosisUids);
            flattenAndTransformDiagnosis.LinkErrorTo(logErrors);
            destPatientDiagnosisUids.LinkErrorTo(logErrors);


            // Trả về pipeline với tất cả các đích
            return new EtlPipeline(
                Source: source,
                Destinations: [destPatientOrders, destPatientOrderItems, destPatientDiagnosisUids, destDispenseBatchDetail],
                ErrorDestination: logErrors,
                SqlStoredProcedureName: "sp_MergePatientOrdersData"
            );
        }
    }
}