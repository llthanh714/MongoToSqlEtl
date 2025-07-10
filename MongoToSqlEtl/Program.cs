using ETLBox.SqlServer;
using MongoDB.Driver;
using MongoToSqlEtl.Jobs;
using Serilog;

namespace MongoToSqlEtl
{
    public class Program
    {
        public static async Task Main()
        {
            // 1. Cấu hình Serilog
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug().Enrich.FromLogContext().WriteTo.Console()
                .WriteTo.File("logs/etl-log-.txt", rollingInterval: RollingInterval.Day,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            try
            {
                Log.Information("--- BẮT ĐẦU PHIÊN LÀM VIỆC ETL ---");

                // 2. Tải cấu hình và tạo kết nối
                var config = LoadConfiguration();
                var sqlConnectionManager = CreateSqlConnectionManager(config);
                var mongoClient = CreateMongoDbClient(config);

                // 3. Khởi tạo và thực thi Job ETL cụ thể
                var patientOrdersJob = new PatientOrdersEtlJob(sqlConnectionManager, mongoClient);
                await patientOrdersJob.RunAsync();

                Log.Information("--- KẾT THÚC PHIÊN LÀM VIỆC ETL THÀNH CÔNG ---");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Ứng dụng ETL đã gặp lỗi nghiêm trọng và bị dừng lại.");
                throw; // Ném lại lỗi để có exit code khác 0
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
