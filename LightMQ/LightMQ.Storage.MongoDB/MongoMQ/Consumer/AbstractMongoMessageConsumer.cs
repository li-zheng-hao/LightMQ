using LightMQ.Transport;

namespace SW.Core.MongoMQ.Consumer;

public abstract class AbstractMongoMessageConsumer:Microsoft.Extensions.Hosting.BackgroundService
{
    public abstract MongoConsumerOptions GetOptions();

    public abstract Task ConsumeAsync(string message, CancellationToken cancellationToken);

    public Message? CurrentMessage { get; set; }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = GetOptions();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                CurrentMessage = await DB.UpdateAndGet<Message>()
                    .Match(m => m.Status == MessageStatus.Waiting && m.Topic == options.Topic)
                    .Modify(m => m.Status, MessageStatus.Processing)
                    .ExecuteAsync();
            
                if (CurrentMessage == null)
                {
                    await Task.Delay(options.PollInterval, stoppingToken);
                    continue;                
                }
        
                await ConsumeAsync(CurrentMessage.Data,stoppingToken);
        
                await DB.Update<Message>()
                    .Match(m => m.ID == CurrentMessage.ID)
                    .Modify(m => m.Status, MessageStatus.Completed)
                    .ExecuteAsync();
        
            }
        }
        catch (TaskCanceledException)
        {
            if (CurrentMessage?.Status == MessageStatus.Processing)
            {
                await DB.Update<MongoMessage>()
                    .Match(m => m.ID == CurrentMessage.ID)
                    .Modify(m => m.Status, MessageStatus.Waiting)
                    .ExecuteAsync();
            }
        }
    }

}