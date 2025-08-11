// In file: Services/IJobConfigurationService.cs
using MongoToSqlEtl.Common;

namespace MongoToSqlEtl.Services
{
    /// <summary>
    /// Defines the contract for the service responsible for retrieving job configurations.
    /// </summary>
    public interface IJobConfigurationService
    {
        /// <summary>
        /// Gets all enabled job configurations from the data source.
        /// </summary>
        /// <returns>A collection of JobSettings objects.</returns>
        Task<IEnumerable<JobSettings>> GetEnabledJobsAsync();
    }
}