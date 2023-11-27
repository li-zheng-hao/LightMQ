using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.BackgroundService;

public class ClearOldMessagesBackgroundService : Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly ILogger<ClearOldMessagesBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;
    private readonly IOptions<LightMQOptions> _options;

    public ClearOldMessagesBackgroundService(ILogger<ClearOldMessagesBackgroundService> logger,
        IStorageProvider storageProvider,IOptions<LightMQOptions> options)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _storageProvider.ClearOldMessagesAsync(stoppingToken);

                _logger.LogDebug("clear old messages success");

                await Task.Delay(_options.Value.MessageExpireDuration, stoppingToken);
            }
        }
        catch (TaskCanceledException) { }
    }
}