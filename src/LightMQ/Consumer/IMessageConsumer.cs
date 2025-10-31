using LightMQ.Options;

namespace LightMQ.Consumer;

public interface IMessageConsumer
{
    ConsumerOptions GetOptions();

    Task<ConsumeResult> ConsumeAsync(string message, CancellationToken cancellationToken);

}