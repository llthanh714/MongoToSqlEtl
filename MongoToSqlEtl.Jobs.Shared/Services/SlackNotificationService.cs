using Serilog;
using System.Text;
using System.Text.Json;

namespace MongoToSqlEtl.Jobs.Services
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

        public async Task SendFailedRecordsSummaryAsync(string jobName, List<string> recordIds)
        {
            if (_webhookUrl == null || recordIds.Count == 0) return;

            // Để tránh tin nhắn quá dài, có thể giới hạn số lượng ID hiển thị
            const int maxIdsToShow = 20;
            var idsToShow = recordIds.Take(maxIdsToShow).ToList();
            var moreIdsCount = recordIds.Count - idsToShow.Count;

            var idListText = string.Join("\n", idsToShow.Select(id => $"`{id}`"));
            if (moreIdsCount > 0)
            {
                idListText += $"\n... và {moreIdsCount} ID khác.";
            }

            var payload = new
            {
                text = $"⚠️ {recordIds.Count} dòng dữ liệu bị lỗi trong Job: *{jobName}*",
                blocks = new object[]
                {
                    new {
                        type = "header",
                        text = new { type = "plain_text", text = $"⚠️ Tóm tắt lỗi dữ liệu: {jobName}" }
                    },
                    new {
                        type = "section",
                        text = new {
                            type = "mrkdwn",
                            text = $"*Job:* `{jobName}`\n*Tổng số lỗi:* {recordIds.Count}\n*Thời gian:* {DateTime.Now:dd/MM/yyyy HH:mm:ss}"
                        }
                    },
                    new { type = "divider" },
                    new {
                        type = "section",
                        text = new { type = "mrkdwn", text = "*Danh sách ID bị lỗi (tối đa 20):*\n" + idListText }
                    },
                    new {
                        type = "context",
                        elements = new object[]
                        {
                            new { type = "mrkdwn", text = "Vui lòng kiểm tra log để xem chi tiết từng lỗi." }
                        }
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