namespace LightMQ.Publisher;

public interface IMessagePublisher
{
    Task PublishAsync<T>(string topic,T message) where T : class;
}