using Serilog;
using System.Text;
using System.Text.Json;

namespace MongoToSqlEtl.Services
{
    /// <summary>
    /// An implementation of INotificationService that sends messages to Slack via Incoming Webhooks.
    /// </summary>
    public class SlackNotificationService : INotificationService
    {
        private readonly HttpClient _httpClient = new();
        private readonly string? _webhookUrl;

        public SlackNotificationService(string? webhookUrl)
        {
            if (string.IsNullOrWhiteSpace(webhookUrl) || !Uri.IsWellFormedUriString(webhookUrl, UriKind.Absolute))
            {
                Log.Warning("[Slack] SlackWebhookUrl is not configured or invalid. Notifications will be disabled.");
                _webhookUrl = null;
                return;
            }
            _webhookUrl = webhookUrl;
        }

        public async Task SendFatalErrorAsync(string jobName, Exception exception)
        {
            if (_webhookUrl == null) return;

            var payload = new
            {
                text = $"🚨 Lỗi nghiêm trọng trong Job ETL: *{jobName}*",
                blocks = new object[]
                {
                    new {
                        type = "header",
                        text = new { type = "plain_text", text = $"🚨 Lỗi nghiêm trọng: {jobName}" }
                    },
                    new { type = "divider" },
                    new {
                        type = "section",
                        text = new {
                            type = "mrkdwn",
                            text = $"*Thời gian:* {DateTime.Now:dd/MM/yyyy HH:mm:ss}\n*Thông báo:* Ứng dụng đã dừng lại do một lỗi không thể phục hồi."
                        }
                    },
                    new {
                        type = "section",
                        text = new { type = "mrkdwn", text = "*Chi tiết lỗi:*\n```" + exception.ToString() + "```" }
                    }
                }
            };
            await SendMessageAsync(payload);
        }

        public async Task SendRecordErrorAsync(string jobName, string recordId, Exception exception)
        {
            if (_webhookUrl == null) return;

            var payload = new
            {
                text = $"⚠️ Lỗi dòng dữ liệu trong Job: *{jobName}*",
                blocks = new object[]
                {
                    new {
                        type = "section",
                        text = new {
                            type = "mrkdwn",
                            text = $"*Job:* `{jobName}`\n*Record ID:* `{recordId}`\n*Thời gian:* {DateTime.Now:dd/MM/yyyy HH:mm:ss}"
                        }
                    },
                    new {
                        type = "section",
                        text = new { type = "mrkdwn", text = "*Chi tiết lỗi:*\n```" + exception.Message + "```" }
                    }
                }
            };
            await SendMessageAsync(payload);
        }

        private async Task SendMessageAsync(object payload)
        {
            try
            {
                var jsonPayload = JsonSerializer.Serialize(payload);
                var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync(_webhookUrl, content);
                if (!response.IsSuccessStatusCode)
                {
                    var responseBody = await response.Content.ReadAsStringAsync();
                    Log.Error("[Slack] Gửi thông báo thất bại. Status: {StatusCode}, Response: {ResponseBody}", response.StatusCode, responseBody);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[Slack] Đã có lỗi xảy ra khi gửi thông báo.");
            }
        }
    }
}