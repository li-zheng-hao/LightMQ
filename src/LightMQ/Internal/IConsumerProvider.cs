namespace LightMQ.Internal;

public interface IConsumerProvider
{
    void ScanConsumers();

    List<ConsumerInfo> GetConsumerInfos();
}