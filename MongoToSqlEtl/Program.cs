using ETLBox;
using ETLBox.SqlServer;
using Hangfire;
using Hangfire.Console;
using Hangfire.Dashboard.BasicAuthorization;
using Hangfire.SqlServer;
using MongoDB.Driver;
using MongoToSqlEtl.Jobs;
using MongoToSqlEtl.Services;
using Serilog;

// --- STEP 1: Cấu hình Serilog ---
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File("Logs/etl-log-.txt", rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

try
{
    Log.Information("Starting ETL Host application initialization..");

    var builder = WebApplication.CreateBuilder(args);

    // Kiểm tra và lấy mật khẩu từ biến môi trường
    var sqlPassword = Environment.GetEnvironmentVariable("__DB_PASSWORD__", EnvironmentVariableTarget.Machine);
    var mongoPassword = Environment.GetEnvironmentVariable("__MONGOGDB_PASSWORD__", EnvironmentVariableTarget.Machine);
    var hangfireDashboardPassword = Environment.GetEnvironmentVariable("__HANGFIRE_DASHBOARD_PASSWORD__", EnvironmentVariableTarget.Machine);

    if (string.IsNullOrWhiteSpace(sqlPassword))
        throw new Exception("__DB_PASSWORD__ environment variable is not set.");

    if (string.IsNullOrWhiteSpace(mongoPassword))
        throw new Exception("__MONGOGDB_PASSWORD__ environment variable is not set.");

    if (string.IsNullOrWhiteSpace(hangfireDashboardPassword))
        throw new Exception("__HANGFIRE_DASHBOARD_PASSWORD__ environment variable is not set.");

    // --- STEP 2: Tích hợp Serilog vào ASP.NET Core ---
    builder.Host.UseSerilog();

    // --- STEP 3: Đăng ký các dịch vụ với Dependency Injection ---
    // Đăng ký IConfiguration để có thể inject vào các lớp khác
    builder.Services.AddSingleton(builder.Configuration);

    // Đăng ký các kết nối DB dưới dạng Singleton
    builder.Services.AddSingleton<IConnectionManager>(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        string? cs = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("Connection string 'SqlServer' not found.");
        return new SqlConnectionManager(cs.Replace("__DB_PASSWORD__", sqlPassword));
    });

    builder.Services.AddSingleton(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        string? cs = config.GetConnectionString("MongoDb") ?? throw new InvalidOperationException("Connection string 'MongoDb' not found.");
        return new MongoClient(cs.Replace("__MONGOGDB_PASSWORD__", mongoPassword));
    });

    // Đăng ký các dịch vụ khác
    builder.Services.AddSingleton<INotificationService, SlackNotificationService>(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        return new SlackNotificationService(config["NotificationSettings:SlackWebhookUrl"]);
    });

    // Đăng ký các Job ETL
    // Dùng AddTransient để mỗi lần Hangfire chạy job, nó sẽ tạo một instance mới.
    builder.Services.AddTransient<PatientOrdersEtlJob>();

    // --- STEP 4: Cấu hình Hangfire ---
    var hangfireConfig = builder.Configuration.GetConnectionString("Hangfire");
    if (string.IsNullOrWhiteSpace(hangfireConfig))
        throw new InvalidOperationException("Connection string 'Hangfire' not found.");

    builder.Services.AddHangfire(configuration => configuration
        .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
        .UseSimpleAssemblyNameTypeSerializer()
        .UseRecommendedSerializerSettings()
        .UseSqlServerStorage(hangfireConfig.Replace("__DB_PASSWORD__", sqlPassword), new SqlServerStorageOptions
        {
            JobExpirationCheckInterval = TimeSpan.FromDays(15),
            CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
            SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
            QueuePollInterval = TimeSpan.Zero,
            UseRecommendedIsolationLevel = true,
            DisableGlobalLocks = true // Recommended for performance
        })
        .UseConsole()); // Tích hợp Hangfire.Console

    // Thêm Hangfire Server để xử lý các job trong background
    builder.Services.AddHangfireServer();

    // --- STEP 5: Build ứng dụng ---
    var app = builder.Build();

    // --- STEP 6: Cấu hình Request Pipeline và Dashboard ---
    app.UseSerilogRequestLogging();

    var hangfireOptions = new DashboardOptions
    {
        Authorization =
        [
            new BasicAuthAuthorizationFilter(new BasicAuthAuthorizationFilterOptions
            {
                RequireSsl = true,
                LoginCaseSensitive = true,
                Users =
                [
                    new BasicAuthAuthorizationUser
                    {
                        Login = "admin",
                        PasswordClear = hangfireDashboardPassword
                    }
                ]
            })
        ]
    };

    app.UseHangfireDashboard("/hangfire", hangfireOptions);

    // --- STEP 7: Đăng ký các Job định kỳ (Recurring Jobs) ---
    // Job này sẽ được tự động thêm/cập nhật khi ứng dụng khởi động
    RecurringJob.AddOrUpdate<PatientOrdersEtlJob>(
        "minutely-patientorder",
        service => service.RunAsync(null), "*/2 * * * *",
            new RecurringJobOptions
            {
                TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Asia/Bangkok"),
            }
    );

    Log.Information("Application initialization completed. Hangfire Dashboard is running at /hangfire.");

    // --- STEP 8: Chạy ứng dụng ---
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "The ETL application's host encountered a critical error during initialization.");
}
finally
{
    Log.CloseAndFlush();
}