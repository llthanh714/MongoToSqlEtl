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
        /// Số lượng bản ghi tối đa để xử lý trong một lần chạy job.
        /// </summary>
        public int MaxRecordsPerJob { get; set; } = 1000;

        /// <summary>
        /// Cấu hình cho việc chạy backfill (lấy lại dữ liệu lịch sử).
        /// </summary>
        public BackfillSettings Backfill { get; set; } = new();
    }

    /// <summary>
    /// Cài đặt cho chế độ Backfill.
    /// </summary>
    public class BackfillSettings
    {
        /// <summary>
        /// Kích hoạt chế độ backfill.
        /// </summary>
        public bool Enabled { get; set; }
    }
}