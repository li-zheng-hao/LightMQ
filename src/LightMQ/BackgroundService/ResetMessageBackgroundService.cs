using System.Security.AccessControl;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.BackgroundService;

public class ResetMessageBackgroundService : IBackgroundService
{
    private readonly ILogger<ResetMessageBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;
    private readonly IOptions<LightMQOptions> _options;
    private readonly IConsumerProvider _consumerProvider;

    public ResetMessageBackgroundService(
        ILogger<ResetMessageBackgroundService> logger,
        IStorageProvider storageProvider,
        IOptions<LightMQOptions> options,
        IConsumerProvider consumerProvider
    )
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _options = options;
        this._consumerProvider = consumerProvider;
    }

    public async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var consumers = _consumerProvider.GetConsumerInfos();
            if (consumers.All(it => it.ConsumerOptions.ResetInterval == null))
            {
                _logger.LogDebug("所有消费者都没有配置重置时间间隔，不重置超时消息");
                return;
            }
            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var consumer in consumers)
                {
                    if (consumer.ConsumerOptions.ResetInterval == null)
                        continue;
                    await _storageProvider.ResetOutOfDateMessagesAsync(
                        consumer.ConsumerOptions.Topic,
                        DateTime.Now.Subtract(consumer.ConsumerOptions.ResetInterval!.Value),
                        stoppingToken
                    );
                }

                _logger.LogDebug("重置超时消息状态完成");

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
        catch (TaskCanceledException) { }
    }
}
