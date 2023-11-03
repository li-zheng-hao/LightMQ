using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.Transport;
using Microsoft.Extensions.Logging;

namespace LightMQ.Consumer;

public abstract class MessageConsumerBase:Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly ILogger<MessageConsumerBase> _logger;
    private readonly IStorageProvider _storageProvider;

    public MessageConsumerBase(ILogger<MessageConsumerBase> logger,IStorageProvider storageProvider)
    {
        _logger = logger;
        _storageProvider = storageProvider;
    }
    public abstract ConsumerOptions GetOptions();

    public abstract Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken);

    public Message? CurrentMessage { get; set; }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = GetOptions();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                CurrentMessage = await _storageProvider.PollNewMessageAsync(options.Topic, stoppingToken);
                if (CurrentMessage == null)
                {
                    await Task.Delay(options.PollInterval, stoppingToken);
                    continue;                
                }
                CurrentMessage.Status=MessageStatus.Processing;
                try
                {
                    var result=await ConsumeAsync(CurrentMessage.Data, stoppingToken);
                    if (result)
                    {
                        await _storageProvider.AckMessageAsync(CurrentMessage, stoppingToken);
                        continue;
                    }
                }
                catch (Exception e)
                {
                    if (e is TaskCanceledException) throw;
                    _logger.LogError(e,$"{GetOptions().Topic}消费消息异常");
                }
                await _storageProvider.NackMessageAsync(CurrentMessage, stoppingToken);

                CurrentMessage = null;

            }
        }
        catch (TaskCanceledException)
        {
            if (CurrentMessage?.Status == MessageStatus.Processing)
            {
                await _storageProvider.ResetMessageAsync(CurrentMessage);
            }
        }
    }

    
}
