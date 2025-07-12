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
        Task SendFatalErrorAsync(string jobName, Exception exception);

        /// <summary>
        /// Sends a summary notification for a batch of failed records.
        /// </summary>
        Task SendFailedRecordsSummaryAsync(string jobName, List<string> recordIds);
    }
}