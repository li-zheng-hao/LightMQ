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
                await Task.Delay(GetDelayToNextRunTime(), stoppingToken);

                if (stoppingToken.IsCancellationRequested)
                    break;

                await _storageProvider.ClearOldMessagesAsync(stoppingToken);

                _logger.LogDebug("清除历史消息完成");
            }
        }
        catch (TaskCanceledException) { }
    }

    /// <summary>
    /// 计算距下次执行清理的等待时间，默认每天凌晨4点执行一次。
    /// 独立成虚方法便于测试覆盖，不改变生产行为。
    /// </summary>
    protected virtual TimeSpan GetDelayToNextRunTime()
    {
        var now = DateTime.Now;
        var nextRun = now.Date.AddHours(4);
        if (now > nextRun)
            nextRun = nextRun.AddDays(1);

        return nextRun - now;
    }
}