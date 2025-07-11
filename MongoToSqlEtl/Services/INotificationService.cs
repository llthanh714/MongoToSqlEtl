namespace MongoToSqlEtl.Services
{
    /// <summary>
    /// Defines the contract for a notification service.
    /// </summary>
    public interface INotificationService
    {
        /// <summary>
        /// Sends a notification for a catastrophic failure of an ETL job.
        /// </summary>
        /// <param name="jobName">The name of the job that failed.</param>
        /// <param name="exception">The exception that caused the failure.</param>
        Task SendFatalErrorAsync(string jobName, Exception exception);

        /// <summary>
        /// Sends a notification for a single failed record.
        /// </summary>
        /// <param name="jobName">The name of the job where the error occurred.</param>
        /// <param name="recordId">The ID of the failed record.</param>
        /// <param name="exception">The exception that occurred.</param>
        Task SendRecordErrorAsync(string jobName, string recordId, Exception exception);
    }
}