using Hangfire;
using Hangfire.Console;
using Hangfire.Dashboard.BasicAuthorization;
using Hangfire.SqlServer;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using MongoToSqlEtl.Jobs.Jobs;
using Serilog;
using System.Net;
using System.Security.Cryptography.X509Certificates;

try
{
    var builder = WebApplication.CreateBuilder(args);

    // Kiểm tra và lấy mật khẩu từ biến môi trường
    var sqlPassword = Environment.GetEnvironmentVariable("__DB_PASSWORD__", EnvironmentVariableTarget.Machine);
    var hangfireDashboardPassword = Environment.GetEnvironmentVariable("__HANGFIRE_DASHBOARD_PASSWORD__", EnvironmentVariableTarget.Machine);
    var kestrelCertPassword = Environment.GetEnvironmentVariable("__KESTREL_CERT_PASSWORD__", EnvironmentVariableTarget.Machine);


    if (string.IsNullOrWhiteSpace(sqlPassword))
        throw new Exception("__DB_PASSWORD__ environment variable is not set.");

    if (string.IsNullOrWhiteSpace(hangfireDashboardPassword))
        throw new Exception("__HANGFIRE_DASHBOARD_PASSWORD__ environment variable is not set.");

    if (string.IsNullOrWhiteSpace(kestrelCertPassword))
        throw new Exception("__KESTREL_CERT_PASSWORD__ environment variable is not set.");

    builder.Host.UseSerilog();

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
            QueuePollInterval = TimeSpan.FromSeconds(30),
            UseRecommendedIsolationLevel = true,
            DisableGlobalLocks = true // Recommended for performance
        })
        .UseConsole());


    _ = builder.WebHost.ConfigureKestrel(serverOptions =>
    {
        var certConfig = builder.Configuration.GetSection("Kestrel:Certificate");
        var certPath = certConfig["Path"];
        // Replace placeholder with password from environment variable
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
            _ = listenOptions.UseHttps();
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

    RecurringJob.AddOrUpdate<PatientOrdersEtlJob>(
        "minutely-patientorder",
        service => service.RunAsync(null), "*/2 * * * *",
            new RecurringJobOptions
            {
                TimeZone = TimeZoneInfo.FindSystemTimeZoneById("Asia/Bangkok"),
            }
    );

    // BackgroundJob.Enqueue<PatientOrdersEtlJob>(service => service.RunAsync(null));

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