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
            PollInterval = TimeSpan.FromSeconds(2),
            RetryCount = 2,
            RetryInterval = TimeSpan.FromSeconds(5)
        };
    }

    public override async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        _logger.LogInformation($"call {message}");
        throw new Exception("1");
        return false;
        return true;
    }

    public TestConsumer(ILogger<TestConsumer> logger, IStorageProvider storageProvider) : base(logger, storageProvider)
    {
        
    }
}