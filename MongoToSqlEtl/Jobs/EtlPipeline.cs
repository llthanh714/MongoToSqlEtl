using ETLBox;
using ETLBox.DataFlow;
using System.Dynamic;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// Một record để chứa các component chính của một pipeline ETL,
    /// giúp truyền chúng giữa các phương thức một cách dễ dàng.
    /// </summary>
    public record EtlPipeline(
        IDataFlowSource<ExpandoObject> Source,
        IEnumerable<IDataFlowDestination<ExpandoObject>> Destinations,
        CustomDestination<ETLBoxError> ErrorDestination
    );
}