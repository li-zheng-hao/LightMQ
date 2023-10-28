using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.Transport;

namespace LightMQ.Consumer;

public abstract class MessageConsumerBase:Microsoft.Extensions.Hosting.BackgroundService
{
    private readonly IStorageProvider _storageProvider;

    public MessageConsumerBase(IStorageProvider storageProvider)
    {
        _storageProvider = storageProvider;
    }
    public abstract ConsumerOptions GetOptions();

    public abstract Task ConsumeAsync(string message, CancellationToken cancellationToken);

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
        
                await ConsumeAsync(CurrentMessage.Data,stoppingToken);

                await _storageProvider.AckMessageAsync(CurrentMessage, stoppingToken);
                
                CurrentMessage = null;

            }
        }
        catch (TaskCanceledException)
        {
            if (CurrentMessage?.Status == MessageStatus.Processing)
            {
                await _storageProvider.NackMessageAsync(CurrentMessage);
            }
        }
    }

    
}
