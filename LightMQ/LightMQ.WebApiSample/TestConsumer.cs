using LightMQ.Consumer;
using LightMQ.Options;
using LightMQ.Storage;

namespace LightMQ.WebApiSample;

public class TestConsumer:MessageConsumerBase
{
  
    public override ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            Topic = "test",
            PollInterval = TimeSpan.FromSeconds(2)
        };
    }

    public override async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        return true;
    }

    public TestConsumer(ILogger<TestConsumer> logger, IStorageProvider storageProvider) : base(logger, storageProvider)
    {
    }
}