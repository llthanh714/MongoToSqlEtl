namespace MongoToSqlEtl.Data
{
    // Ánh xạ với bảng dbo.EtlJobSettings
    public class DbJobSetting
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string JobType { get; set; } = string.Empty;
        public string Cron { get; set; } = string.Empty;
        public string TimeZone { get; set; } = string.Empty;
        public bool Enabled { get; set; }
        public int MaxRecordsPerJob { get; set; }
        public bool BackfillEnabled { get; set; }
        public DateTime? BackfillUntilDateUtc { get; set; }
        public string? MergeStoredProcedure { get; set; }
    }

    // Ánh xạ với bảng dbo.EtlTableMappings
    public class DbTableMapping
    {
        public int Id { get; set; }
        public int JobSettingsId { get; set; } // Khóa ngoại
        public string MongoFieldName { get; set; } = string.Empty;
        public string SqlTableName { get; set; } = string.Empty;
        public string? ParentMongoFieldName { get; set; }
        public string? ForeignKeyName { get; set; }
        public string ParentIdSourceField { get; set; } = "_id";
    }
}