using LightMQ.Consumer;
using LightMQ.Options;
using LightMQ.Transport;

namespace LightMQ.WebApiSample;

public class Test2Consumer:IMessageConsumer,IDisposable
{
    public Test2Consumer()
    {
        Console.WriteLine("初始化Test2Consumer");
    }
    public ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            ParallelNum = 2,
            Topic = "test"
        };
    }

    public async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine(message);
        await Task.Delay(10000,cancellationToken);
        return true;
    }

    public void Dispose()
    {
        Console.WriteLine("销毁Test2Consumer");

    }
}