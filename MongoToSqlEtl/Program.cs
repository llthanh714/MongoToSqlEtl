using ETLBox.SqlServer;
using MongoDB.Driver;
using MongoToSqlEtl.Jobs;
using MongoToSqlEtl.Services;
using Serilog;

namespace MongoToSqlEtl
{
    public class Program
    {
        public static async Task Main()
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug().Enrich.FromLogContext().WriteTo.Console()
                .WriteTo.File("Logs/etl-log-.txt", rollingInterval: RollingInterval.Day,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            IConfiguration config = LoadConfiguration();
            SlackNotificationService notificationService = new(config["NotificationSettings:SlackWebhookUrl"]);
            string jobName = "PatientOrdersETL"; // Define a name for the job

            try
            {
                Log.Information("--- BẮT ĐẦU PHIÊN LÀM VIỆC ETL CHO JOB: {JobName} ---", jobName);

                var sqlConnectionManager = CreateSqlConnectionManager(config);
                var mongoClient = CreateMongoDbClient(config);

                var patientOrdersJob = new PatientOrdersEtlJob(sqlConnectionManager, mongoClient, notificationService);
                await patientOrdersJob.RunAsync();

                Log.Information("--- KẾT THÚC PHIÊN LÀM VIỆC ETL THÀNH CÔNG CHO JOB: {JobName} ---", jobName);
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Ứng dụng ETL đã gặp lỗi nghiêm trọng và bị dừng lại cho job: {JobName}", jobName);
                await notificationService.SendFatalErrorAsync(jobName, ex);
                throw;
            }
            finally
            {
                await Log.CloseAndFlushAsync();
            }
        }

        private static IConfiguration LoadConfiguration() => new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

        private static SqlConnectionManager CreateSqlConnectionManager(IConfiguration config)
        {
            string? cs = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("Connection string 'SqlServer' not found.");
            return new SqlConnectionManager(cs);
        }

        private static MongoClient CreateMongoDbClient(IConfiguration config)
        {
            string? cs = config.GetConnectionString("MongoDb") ?? throw new InvalidOperationException("Connection string 'MongoDb' not found.");
            return new MongoClient(cs);
        }
    }
}
