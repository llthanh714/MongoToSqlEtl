using Hangfire;
using Hangfire.Server;
using Hangfire.Storage;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Services;
using Serilog;
using System.Linq.Expressions;

namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// A special job to synchronize the job configurations from the database with Hangfire's recurring jobs.
    /// </summary>
    public class JobReloaderJob(IJobConfigurationService jobConfigService, IServiceProvider serviceProvider)
    {

        // ID của job hệ thống, để tránh nó tự xóa mình
        private const string RELOADER_JOB_ID = "System: Reload All Jobs";

        /// <summary>
        /// Reloads all jobs from the database, adding/updating new ones and removing obsolete ones.
        /// </summary>
        [DisableConcurrentExecution(timeoutInSeconds: 120)]
        public async Task ReloadAllJobs()
        {
            Log.Information("[JobReloader] Starting to synchronize all recurring jobs from the database...");

            // 1. Get all recurring jobs currently registered in Hangfire
            List<RecurringJobDto> hangfireJobs = JobStorage.Current.GetConnection().GetRecurringJobs();
            var hangfireJobIds = hangfireJobs.Select(j => j.Id).ToHashSet();
            Log.Information("[JobReloader] Found {Count} existing jobs in Hangfire.", hangfireJobIds.Count);

            // 2. Get all enabled job configurations from the database
            var dbJobs = (await jobConfigService.GetEnabledJobsAsync()).ToList();
            var dbJobIds = dbJobs.Select(j => j.Name).ToHashSet();
            Log.Information("[JobReloader] Found {Count} enabled jobs in the database configuration.", dbJobIds.Count);

            // 3. Add or Update jobs from DB to Hangfire
            foreach (var jobSetting in dbJobs)
            {
                try
                {
                    using var scope = serviceProvider.CreateScope();
                    var jobType = Type.GetType(jobSetting.JobType);
                    if (jobType == null)
                    {
                        Log.Warning("[JobReloader] Job Type '{JobType}' for job '{JobName}' not found. Skipping.", jobSetting.JobType, jobSetting.Name);
                        continue;
                    }

                    var jobInstance = scope.ServiceProvider.GetRequiredService(jobType);
                    var runAsyncMethod = jobType.GetMethod("RunAsync", [typeof(PerformContext), typeof(JobSettings)]);
                    if (runAsyncMethod == null)
                    {
                        Log.Warning("[JobReloader] Suitable RunAsync method not found for job '{JobName}'. Skipping.", jobSetting.Name);
                        continue;
                    }

                    var instanceConst = Expression.Constant(jobInstance);
                    var nullContext = Expression.Constant(null, typeof(PerformContext));
                    var jobSettingsConst = Expression.Constant(jobSetting, typeof(JobSettings));
                    var call = Expression.Call(instanceConst, runAsyncMethod, nullContext, jobSettingsConst);
                    var lambda = Expression.Lambda<Func<Task>>(call);

                    RecurringJob.AddOrUpdate(
                        jobSetting.Name,
                        lambda,
                        jobSetting.Cron,
                        new RecurringJobOptions
                        {
                            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(jobSetting.TimeZone)
                        }
                    );
                    Log.Information("[JobReloader] Successfully added or updated job: '{JobName}'.", jobSetting.Name);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "[JobReloader] Failed to add or update job: '{JobName}'.", jobSetting.Name);
                }
            }

            // 4. Remove jobs that are in Hangfire but not in the DB configuration
            var jobsToRemove = hangfireJobIds.Except(dbJobIds);
            foreach (var jobIdToRemove in jobsToRemove)
            {
                if (jobIdToRemove == RELOADER_JOB_ID)
                {
                    Log.Information("[JobReloader] Skipping the removal of the reloader job itself.");
                    continue; // Bỏ qua và không xóa
                }
                RecurringJob.RemoveIfExists(jobIdToRemove);
                Log.Information("[JobReloader] Successfully removed job no longer in configuration: '{JobId}'.", jobIdToRemove);
            }

            Log.Information("[JobReloader] Recurring job synchronization completed.");
        }
    }
}