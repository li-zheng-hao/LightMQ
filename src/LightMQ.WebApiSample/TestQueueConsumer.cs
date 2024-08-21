using LightMQ.Consumer;
using LightMQ.Options;
using LightMQ.Transport;

namespace LightMQ.WebApiSample;

public class TestQueueConsumer:IMessageConsumer
{

    public ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            ParallelNum = 1,
            Topic = "test-queue",
            EnableRandomQueue = true
        };
    }

    public async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine("消费消息"+message);
        await Task.Delay(1_000,cancellationToken);
        return true;
    }

  
}