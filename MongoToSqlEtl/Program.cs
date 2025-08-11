using ETLBox;
using ETLBox.Licensing;
using ETLBox.SqlServer;
using Hangfire;
using Hangfire.Console;
using Hangfire.Dashboard.BasicAuthorization;
using Hangfire.Server;
using Hangfire.SqlServer;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using MongoDB.Driver;
using MongoToSqlEtl.Common;
using MongoToSqlEtl.Jobs;
using MongoToSqlEtl.Services;
using Serilog;
using System.Linq.Expressions;
using System.Net;
using System.Security.Cryptography.X509Certificates;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File("logs/etl-log-.txt", rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

// The license key remains unchanged
LicenseService.CurrentKey = "2025-09-22|TRIAL|ONLY FOR PERSONAL OR TESTING PURPOSES|CUSTOMER:Le Long Thanh|MAIL:llthanh714@gmail.com||jKQA+uo+BcY7ZHBXmKMyf6gdOIhhD34RepQHEMERQOdQ2Kj6t6nBLmyZnlh4K8UZmuUZBVxHraOnVrJwelVCjb2fko+Lj9vSXmydzkv1x9YFVK0L06Sm8CMntvMJREsvh97p3rdckV53TWhSH4muPPTdfwuAfjW4C7umRAdO3dE=";

try
{
    Log.Information("Starting ETL Host application initialization..");

    var builder = WebApplication.CreateBuilder(args);

    var sqlPassword = Environment.GetEnvironmentVariable("__DB_PASSWORD__", EnvironmentVariableTarget.Machine);
    var mongoPassword = Environment.GetEnvironmentVariable("__MONGOGDB_PASSWORD__", EnvironmentVariableTarget.Machine);
    var hangfireDashboardPassword = Environment.GetEnvironmentVariable("__HANGFIRE_DASHBOARD_PASSWORD__", EnvironmentVariableTarget.Machine);
    var kestrelCertPassword = Environment.GetEnvironmentVariable("__KESTREL_CERT_PASSWORD__", EnvironmentVariableTarget.Machine);

    if (string.IsNullOrWhiteSpace(sqlPassword)) throw new Exception("__DB_PASSWORD__ environment variable is not set.");
    if (string.IsNullOrWhiteSpace(mongoPassword)) throw new Exception("__MONGOGDB_PASSWORD__ environment variable is not set.");
    if (string.IsNullOrWhiteSpace(hangfireDashboardPassword)) throw new Exception("__HANGFIRE_DASHBOARD_PASSWORD__ environment variable is not set.");
    if (string.IsNullOrWhiteSpace(kestrelCertPassword)) throw new Exception("__KESTREL_CERT_PASSWORD__ environment variable is not set.");

    builder.Host.UseSerilog();
    builder.Services.AddSingleton(builder.Configuration);

    builder.Services.AddSingleton<IConnectionManager>(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        string cs = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("Connection string 'SqlServer' not found.");
        return new SqlConnectionManager(cs.Replace("__DB_PASSWORD__", sqlPassword));
    });

    builder.Services.AddSingleton(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        string cs = config.GetConnectionString("MongoDb") ?? throw new InvalidOperationException("Connection string 'MongoDb' not found.");
        return new MongoClient(cs.Replace("__MONGOGDB_PASSWORD__", mongoPassword));
    });

    builder.Services.AddSingleton<INotificationService, SlackNotificationService>(sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        return new SlackNotificationService(config["NotificationSettings:SlackWebhookUrl"]);
    });

    builder.Services.AddSingleton<IJobConfigurationService, JobConfigurationService>();
    builder.Services.AddTransient<ConfigurableEtlJob>();
    builder.Services.AddTransient<PatientOrdersEtlJob>();

    var hangfireConfig = builder.Configuration.GetConnectionString("Hangfire");
    if (string.IsNullOrWhiteSpace(hangfireConfig)) throw new InvalidOperationException("Connection string 'Hangfire' not found.");

    builder.Services.AddHangfire(configuration => configuration
        .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
        .UseSimpleAssemblyNameTypeSerializer()
        .UseRecommendedSerializerSettings()
        .UseSqlServerStorage(hangfireConfig.Replace("__DB_PASSWORD__", sqlPassword), new SqlServerStorageOptions
        {
            JobExpirationCheckInterval = TimeSpan.FromDays(15),
            CommandBatchMaxTimeout = TimeSpan.FromMinutes(5),
            SlidingInvisibilityTimeout = TimeSpan.FromMinutes(5),
            QueuePollInterval = TimeSpan.FromSeconds(5),
            UseRecommendedIsolationLevel = true,
            DisableGlobalLocks = true
        })
        .UseConsole());

    builder.Services.AddHangfireServer();

    builder.WebHost.ConfigureKestrel(serverOptions =>
    {
        var certConfig = builder.Configuration.GetSection("Kestrel:Certificate");
        var certPath = certConfig["Path"];
        var certPassword = certConfig["Password"]?.Replace("__KESTREL_CERT_PASSWORD__", kestrelCertPassword);

        if (string.IsNullOrEmpty(certPath) || string.IsNullOrEmpty(certPassword))
        {
            Log.Warning("Kestrel certificate path or password is not configured. HTTPS will not be available.");
            return;
        }

        serverOptions.ConfigureHttpsDefaults(https =>
        {
            https.ServerCertificate = new X509Certificate2(certPath, certPassword);
        });

        serverOptions.Listen(IPAddress.Any, 7272, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            listenOptions.UseHttps();
        });
    });

    var app = builder.Build();

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

    app.UseHangfireDashboard("/etl", hangfireOptions);

    using (var serviceScope = app.Services.CreateScope())
    {
        var serviceProvider = serviceScope.ServiceProvider;
        var jobConfigService = serviceProvider.GetRequiredService<IJobConfigurationService>();

        // Get the list of active jobs from the database
        var recurringJobs = await jobConfigService.GetEnabledJobsAsync();

        if (recurringJobs != null && recurringJobs.Any())
        {
            foreach (var jobSetting in recurringJobs)
            {
                try
                {
                    // The logic inside this loop remains exactly the same
                    var jobType = Type.GetType(jobSetting.JobType);
                    if (jobType == null)
                    {
                        Log.Warning("Recurring Job Type '{JobType}' not found. Skipping job registration: '{JobName}'", jobSetting.JobType, jobSetting.Name);
                        continue;
                    }

                    var jobInstance = serviceProvider.GetRequiredService(jobType);

                    var runAsyncMethod = jobType.GetMethod("RunAsync", [typeof(PerformContext), typeof(JobSettings)]);
                    if (runAsyncMethod == null)
                    {
                        Log.Warning("Recurring Job Type '{JobType}' does not have a suitable RunAsync method. Skipping job: '{JobName}'", jobSetting.JobType, jobSetting.Name);
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
                            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(jobSetting.TimeZone),
                        }
                    );

                    Log.Information("Successfully registered/updated job from DB: '{JobName}' with schedule: '{Cron}'", jobSetting.Name, jobSetting.Cron);
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Failed to register recurring job from DB: '{JobName}'.", jobSetting.Name);
                }
            }
        }
        else
        {
            Log.Information("No enabled jobs found in the database to register.");
        }
    }

    Log.Information("Application initialization completed. Hangfire Dashboard is running at /etl.");

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "The ETL application's host encountered a critical error during initialization.");
}
finally
{
    await Log.CloseAndFlushAsync();
}