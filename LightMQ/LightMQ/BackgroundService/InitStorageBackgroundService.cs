using LightMQ.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace LightMQ.BackgroundService;

/// <summary>
/// 初始化数据库表
/// </summary>
public class InitStorageBackgroundService:IHostedService
{
    private readonly ILogger<InitStorageBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;

    public InitStorageBackgroundService(ILogger<InitStorageBackgroundService> logger,IStorageProvider storageProvider)
    {
        _logger = logger;
        _storageProvider = storageProvider;
    }

    protected async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _storageProvider.InitTables(stoppingToken);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return ExecuteAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}