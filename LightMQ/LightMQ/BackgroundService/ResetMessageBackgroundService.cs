using LightMQ.Storage;
using Microsoft.Extensions.Logging;

namespace LightMQ.BackgroundService;

public class ResetMessageBackgroundService:Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly ILogger<ResetMessageBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;

    public ResetMessageBackgroundService(ILogger<ResetMessageBackgroundService> logger,IStorageProvider storageProvider)
    {
        _logger = logger;
        _storageProvider = storageProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _storageProvider.ResetOutOfDateMessagesAsync(stoppingToken);
            
                _logger.LogDebug("reset processing messages success");

                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
            }
        }
        catch (TaskCanceledException)
        {
        }
       
    }

}