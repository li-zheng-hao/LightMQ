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

    public override async Task ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine(message);
        await Task.Delay(Random.Shared.Next(1000, 3000), cancellationToken);
    }
}