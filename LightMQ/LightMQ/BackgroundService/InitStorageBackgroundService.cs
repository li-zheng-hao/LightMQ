using LightMQ.Storage;
using Microsoft.Extensions.Logging;

namespace LightMQ.BackgroundService;

public class InitStorageBackgroundService:Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly ILogger<InitStorageBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;

    public InitStorageBackgroundService(ILogger<InitStorageBackgroundService> logger,IStorageProvider storageProvider)
    {
        _logger = logger;
        _storageProvider = storageProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _storageProvider.InitTables(stoppingToken);
    }

}