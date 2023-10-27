using LightMQ.Storage;
using LightMQ.Transport;
using Microsoft.Extensions.Logging;

namespace LightMQ.BackgroundService;

public class ClearOldMessagesBackgroundService : Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly ILogger<ClearOldMessagesBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;

    public ClearOldMessagesBackgroundService(ILogger<ClearOldMessagesBackgroundService> logger,
        IStorageProvider storageProvider)
    {
        _logger = logger;
        _storageProvider = storageProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _storageProvider.ClearOldMessagesAsync(stoppingToken);
        }
        catch (TaskCanceledException) { }
    }
}