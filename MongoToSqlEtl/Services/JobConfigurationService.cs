using Dapper;
using Microsoft.Data.SqlClient;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Data;
using Serilog;

namespace MongoToSqlEtl.Services
{
    /// <summary>
    /// Implements the service responsible for retrieving job configurations from the database.
    /// </summary>
    public class JobConfigurationService : IJobConfigurationService
    {
        private readonly string _connectionString;

        public JobConfigurationService(IConfiguration configuration)
        {
            var sqlPassword = Environment.GetEnvironmentVariable("__DB_PASSWORD__", EnvironmentVariableTarget.Machine)
                ?? throw new InvalidOperationException("The __DB_PASSWORD__ environment variable is not set.");
            _connectionString = configuration.GetConnectionString("SqlServer")!.Replace("__DB_PASSWORD__", sqlPassword);
        }

        /// <summary>
        /// Retrieves all enabled job configurations from the database.
        /// </summary>
        /// <returns>A collection of JobSettings objects.</returns>
        public async Task<IEnumerable<JobSettings>> GetEnabledJobsAsync()
        {
            Log.Information("Fetching enabled job configurations from the database...");
            var finalJobSettings = new List<JobSettings>();

            using var connection = new SqlConnection(_connectionString);
            try
            {
                // Get all jobs with Enabled status = 1
                var jobSql = "SELECT * FROM dbo.__ETLJobSettings WHERE Enabled = 1;";
                var jobsFromDb = await connection.QueryAsync<DbJobSetting>(jobSql);

                if (!jobsFromDb.Any())
                {
                    Log.Information("No enabled jobs found in the database.");
                    return finalJobSettings;
                }

                // For each job, retrieve its corresponding mappings
                foreach (var jobDb in jobsFromDb)
                {
                    var mappingSql = "SELECT * FROM dbo.__ETLTableMappings WHERE JobSettingsId = @JobId;";
                    var mappingsFromDb = await connection.QueryAsync<TableMapping>(mappingSql, new { JobId = jobDb.Id });

                    // Convert from Db models to the JobSettings model used by the application
                    var jobSetting = new JobSettings
                    {
                        Name = jobDb.Name,
                        JobType = jobDb.JobType,
                        Cron = jobDb.Cron,
                        TimeZone = jobDb.TimeZone,
                        Enabled = jobDb.Enabled,
                        MaxRecordsPerJob = jobDb.MaxRecordsPerJob,
                        Backfill = new BackfillSettings
                        {
                            Enabled = jobDb.BackfillEnabled,
                            BackfillUntilDateUtc = jobDb.BackfillUntilDateUtc
                        },
                        MergeStoredProcedure = jobDb.MergeStoredProcedure,
                        Mappings = [.. mappingsFromDb]
                    };
                    finalJobSettings.Add(jobSetting);
                }
                Log.Information("Successfully fetched {Count} job configurations from the database.", finalJobSettings.Count);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "A critical error occurred while fetching job configurations from the database.");
                // Return an empty list to prevent the application from crashing
                return [];
            }

            return finalJobSettings;
        }
    }
}