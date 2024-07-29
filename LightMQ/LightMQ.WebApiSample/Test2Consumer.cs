using LightMQ.Consumer;
using LightMQ.Options;
using LightMQ.Transport;

namespace LightMQ.WebApiSample;

public class Test2Consumer:IMessageConsumer
{

    public ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            ParallelNum = 1,
            Topic = "test"
        };
    }

    public async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine(message);
        await Task.Delay(60_000,cancellationToken);
        return true;
    }

  
}