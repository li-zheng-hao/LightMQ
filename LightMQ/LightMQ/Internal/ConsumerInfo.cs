using LightMQ.Options;

namespace LightMQ.Internal;

public class ConsumerInfo
{
    public Type ConsumerType { get; set; }
    
    public ConsumerOptions ConsumerOptions { get; set; }
}