using LightMQ.Consumer;
using LightMQ.Options;
using LightMQ.Storage;

namespace LightMQ.WebApiSample;

public class TestConsumer:MessageConsumerBase
{
    public TestConsumer(IStorageProvider storageProvider) : base(storageProvider)
    {
    }

    public override ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            Topic = "test",
            PollInterval = TimeSpan.FromSeconds(2)
        };
    }

    public override Task ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine(message);
        return Task.CompletedTask;
    }
}