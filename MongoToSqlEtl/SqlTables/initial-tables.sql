-- ----------------------------
-- Table structure for __ETLExecutionLog
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[__ETLExecutionLog]') AND type IN ('U'))
	DROP TABLE [dbo].[__ETLExecutionLog]
GO

CREATE TABLE [dbo].[__ETLExecutionLog] (
  [Id] int  IDENTITY(1,1) NOT NULL,
  [SourceCollectionName] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [ExecutionStartTimeUtc] datetime2(7)  NOT NULL,
  [ExecutionEndTimeUtc] datetime2(7)  NULL,
  [WatermarkPreviousModifiedAtUtc] datetime2(7)  NOT NULL,
  [WatermarkLastModifiedAtUtc] datetime2(7)  NOT NULL,
  [Status] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [SourceRecordCount] int  NULL,
  [SuccessRecordCount] int  NULL,
  [FailedRecordCount] int  NULL,
  [ErrorMessage] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [RunDescription] nvarchar(500) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [WatermarkLastId] nvarchar(24) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [JobMode] nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL
)
GO

ALTER TABLE [dbo].[__ETLExecutionLog] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Table structure for __ETLFailedRecords
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[__ETLFailedRecords]') AND type IN ('U'))
	DROP TABLE [dbo].[__ETLFailedRecords]
GO

CREATE TABLE [dbo].[__ETLFailedRecords] (
  [Id] int  IDENTITY(1,1) NOT NULL,
  [SourceCollectionName] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [FailedRecordId] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [Status] nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS DEFAULT 'Pending' NOT NULL,
  [ErrorMessage] nvarchar(max) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [LoggedAtUtc] datetime2(7) DEFAULT getutcdate() NOT NULL,
  [ResolvedAtUtc] datetime2(7)  NULL,
  [RetryCount] int DEFAULT 0 NOT NULL
)
GO

ALTER TABLE [dbo].[__ETLFailedRecords] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Table structure for __ETLJobSettings
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[__ETLJobSettings]') AND type IN ('U'))
	DROP TABLE [dbo].[__ETLJobSettings]
GO

CREATE TABLE [dbo].[__ETLJobSettings] (
  [Id] int  IDENTITY(1,1) NOT NULL,
  [Name] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [JobType] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [Cron] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [TimeZone] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS DEFAULT 'Asia/Bangkok' NOT NULL,
  [Enabled] bit  NOT NULL,
  [MaxRecordsPerJob] int DEFAULT 1000 NOT NULL,
  [BackfillEnabled] bit DEFAULT 0 NOT NULL,
  [MergeStoredProcedure] nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [BackfillUntilDateUtc] datetime2(7)  NULL
)
GO

ALTER TABLE [dbo].[__ETLJobSettings] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Table structure for __ETLTableMappings
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[__ETLTableMappings]') AND type IN ('U'))
	DROP TABLE [dbo].[__ETLTableMappings]
GO

CREATE TABLE [dbo].[__ETLTableMappings] (
  [Id] int  IDENTITY(1,1) NOT NULL,
  [JobSettingsId] int  NOT NULL,
  [MongoFieldName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [SqlTableName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS  NOT NULL,
  [ParentMongoFieldName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [ForeignKeyName] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS  NULL,
  [ParentIdSourceField] nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS DEFAULT '' NOT NULL
)
GO

ALTER TABLE [dbo].[__ETLTableMappings] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Auto increment value for __ETLExecutionLog
-- ----------------------------
DBCC CHECKIDENT ('[dbo].[__ETLExecutionLog]', RESEED, 1)
GO


-- ----------------------------
-- Indexes structure for table __ETLExecutionLog
-- ----------------------------
CREATE NONCLUSTERED INDEX [IX_ETLExecutionLog_WatermarkQuery]
ON [dbo].[__ETLExecutionLog] (
  [SourceCollectionName] ASC,
  [Status] ASC,
  [WatermarkLastModifiedAtUtc] DESC
)
GO


-- ----------------------------
-- Primary Key structure for table __ETLExecutionLog
-- ----------------------------
ALTER TABLE [dbo].[__ETLExecutionLog] ADD CONSTRAINT [PK__EtlExecu__3214EC0766FCD184] PRIMARY KEY CLUSTERED ([Id])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO


-- ----------------------------
-- Auto increment value for __ETLFailedRecords
-- ----------------------------
DBCC CHECKIDENT ('[dbo].[__ETLFailedRecords]', RESEED, 1)
GO


-- ----------------------------
-- Indexes structure for table __ETLFailedRecords
-- ----------------------------
CREATE UNIQUE NONCLUSTERED INDEX [UQ_FailedRecord]
ON [dbo].[__ETLFailedRecords] (
  [SourceCollectionName] ASC,
  [FailedRecordId] ASC
)
GO

CREATE NONCLUSTERED INDEX [IX_ETLFailedRecords_PendingRetry]
ON [dbo].[__ETLFailedRecords] (
  [SourceCollectionName] ASC,
  [Status] ASC
)
INCLUDE ([RetryCount])
GO


-- ----------------------------
-- Primary Key structure for table __ETLFailedRecords
-- ----------------------------
ALTER TABLE [dbo].[__ETLFailedRecords] ADD CONSTRAINT [PK__EtlFaile__3214EC07B53CC4E1] PRIMARY KEY CLUSTERED ([Id])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO


-- ----------------------------
-- Auto increment value for __ETLJobSettings
-- ----------------------------
DBCC CHECKIDENT ('[dbo].[__ETLJobSettings]', RESEED, 1)
GO


-- ----------------------------
-- Uniques structure for table __ETLJobSettings
-- ----------------------------
ALTER TABLE [dbo].[__ETLJobSettings] ADD CONSTRAINT [UQ__EtlJobSe__737584F6B4957674] UNIQUE NONCLUSTERED ([Name] ASC)
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO


-- ----------------------------
-- Primary Key structure for table __ETLJobSettings
-- ----------------------------
ALTER TABLE [dbo].[__ETLJobSettings] ADD CONSTRAINT [PK__EtlJobSe__3214EC0726BD20F2] PRIMARY KEY CLUSTERED ([Id])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO


-- ----------------------------
-- Auto increment value for __ETLTableMappings
-- ----------------------------
DBCC CHECKIDENT ('[dbo].[__ETLTableMappings]', RESEED, 1)
GO


-- ----------------------------
-- Primary Key structure for table __ETLTableMappings
-- ----------------------------
ALTER TABLE [dbo].[__ETLTableMappings] ADD CONSTRAINT [PK__EtlTable__3214EC0780D1ECDC] PRIMARY KEY CLUSTERED ([Id])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO


-- ----------------------------
-- Foreign Keys structure for table __ETLTableMappings
-- ----------------------------
ALTER TABLE [dbo].[__ETLTableMappings] ADD CONSTRAINT [FK_EtlTableMappings_EtlJobSettings] FOREIGN KEY ([JobSettingsId]) REFERENCES [dbo].[__ETLJobSettings] ([Id]) ON DELETE CASCADE ON UPDATE NO ACTION
GO

