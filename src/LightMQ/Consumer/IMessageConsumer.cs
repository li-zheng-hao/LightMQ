using LightMQ.Options;

namespace LightMQ.Consumer;

public interface IMessageConsumer
{
    ConsumerOptions GetOptions();

    Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken);

}