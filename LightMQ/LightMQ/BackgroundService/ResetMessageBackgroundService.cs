using LightMQ.Options;
using LightMQ.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.BackgroundService;

public class ResetMessageBackgroundService:Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly ILogger<ResetMessageBackgroundService> _logger;
    private readonly IStorageProvider _storageProvider;
    private readonly IOptions<LightMQOptions> _options;

    public ResetMessageBackgroundService(ILogger<ResetMessageBackgroundService> logger,IStorageProvider storageProvider,IOptions<LightMQOptions> options)
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
                await _storageProvider.ResetOutOfDateMessagesAsync(stoppingToken);
            
                _logger.LogDebug("reset processing messages success");

                await Task.Delay(_options.Value.MessageTimeoutDuration, stoppingToken);
            }
        }
        catch (TaskCanceledException)
        {
        }
       
    }

}