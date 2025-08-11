namespace MongoToSqlEtl.Common
{
    /// <summary>
    /// Định nghĩa ánh xạ từ một trường trong MongoDB (có thể là document gốc hoặc một mảng con) tới một bảng trong SQL.
    /// </summary>
    public class TableMapping
    {
        /// <summary>
        /// Tên trường trong document MongoDB. Đối với document gốc, có thể để trống hoặc dùng một tên đại diện.
        /// </gsummary>
        public string MongoFieldName { get; set; } = string.Empty;

        /// <summary>
        /// Tên bảng SQL đích.
        /// </summary>
        public string SqlTableName { get; set; } = string.Empty;

        /// <summary>
        /// Tên trường của đối tượng cha chứa mảng này. Null hoặc rỗng cho document gốc (bảng cha).
        /// Ví dụ: "patientorderitems" nằm trong document gốc, nên trường này là null. "dispensebatchdetail" nằm trong "patientorderitems", nên trường này là "patientorderitems".
        /// </summary>
        public string? ParentMongoFieldName { get; set; }

        /// <summary>
        /// Tên cột khóa ngoại trong bảng SQL đích để liên kết với bảng cha.
        /// </summary>
        public string ForeignKeyName { get; set; } = string.Empty;

        /// <summary>
        /// Trường trong đối tượng cha được sử dụng làm giá trị cho khóa ngoại. Mặc định là "_id".
        /// </summary>
        public string ParentIdSourceField { get; set; } = "_id";
    }


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

        /// <summary>
        /// (MỚI) Định nghĩa các ánh xạ từ MongoDB sang SQL để job tự động xây dựng pipeline.
        /// </summary>
        public List<TableMapping> Mappings { get; set; } = [];

        /// <summary>
        /// (MỚI) Tên của Stored Procedure (tùy chọn) để thực thi sau khi tải dữ liệu vào các bảng staging.
        /// </summary>
        public string? MergeStoredProcedure { get; set; }
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