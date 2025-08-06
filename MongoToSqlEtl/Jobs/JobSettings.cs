namespace MongoToSqlEtl.Jobs
{
    /// <summary>
    /// Đại diện cho cấu hình của một recurring job trong file appsettings.json.
    /// </summary>
    public class JobSettings
    {
        /// <summary>
        /// Tên định danh duy nhất của job trong Hangfire.
        /// </summary>
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// Tên đầy đủ của lớp Job và assembly chứa nó.
        /// Ví dụ: "MongoToSqlEtl.Jobs.PatientOrdersEtlJob, MongoToSqlEtl"
        /// </summary>
        public string JobType { get; set; } = string.Empty;

        /// <summary>
        /// Biểu thức Cron để lên lịch cho job.
        /// </summary>
        public string Cron { get; set; } = string.Empty;

        /// <summary>
        /// Timezone để thực thi job.
        /// Ví dụ: "Asia/Bangkok"
        /// </summary>
        public string TimeZone { get; set; } = string.Empty;

        /// <summary>
        /// Cho biết job có được kích hoạt hay không.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Gets or sets the maximum interval, in minutes, for batching operations.
        /// </summary>
        public int MaxBatchIntervalInMinutes { get; set; } = 60;
    }
}