using LightMQ.Consumer;
using LightMQ.Options;

namespace LightMQ.UnitTest.InternalHelpers;

public class FakeConsumer:IMessageConsumer
{
    public bool ReturnResult { get; set; }= true;

    public int Seconds { get; set; } = 0;
    
    public bool ThrowException { get; set; } = false;
    public ConsumerOptions GetOptions()
    {
        return MockHelper.GetFakeConsumerOptions();
    }

    public async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        if (ThrowException)
        {
            throw new Exception("test exception");
        }
        if (Seconds > 0)
        {
            await Task.Delay(Seconds * 1000,cancellationToken);
        }

        return ReturnResult;
    }
}