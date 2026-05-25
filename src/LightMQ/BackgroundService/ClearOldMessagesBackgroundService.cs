using LightMQ.Options;
using LightMQ.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.BackgroundService;

public class ClearOldMessagesBackgroundService : IBackgroundService
{
    private readonly ILogger<ClearOldMessagesBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;
    private readonly IOptions<LightMQOptions> _options;

    public ClearOldMessagesBackgroundService(ILogger<ClearOldMessagesBackgroundService> logger,
        IStorageProvider storageProvider, IOptions<LightMQOptions> options)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _options = options;
    }

    public async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var now = DateTime.Now;
                var nextRun = now.Date.AddHours(4);
                if (now > nextRun)
                    nextRun = nextRun.AddDays(1);

                var delay = nextRun - now;
                await Task.Delay(delay, stoppingToken);

                if (stoppingToken.IsCancellationRequested)
                    break;

                await _storageProvider.ClearOldMessagesAsync(stoppingToken);

                _logger.LogDebug("清除历史消息完成");
            }
        }
        catch (TaskCanceledException) { }
    }
}