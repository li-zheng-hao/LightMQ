using System.Reflection;
using LightMQ.Consumer;
using LightMQ.Internal;
using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace LightMQ.UnitTest.InternalHelpers;

public class MockHelper
{
    public static Mock<Assembly> MockAssemblyWithEmpty()
    {
        var assembly = new Mock<Assembly>();
        assembly
            .Setup(a => a.GetTypes())
            .Returns(new Type[0]);
        return assembly;
    }

    public static List<ConsumerInfo> GetFakeConsumerInfos()
    {
        return new List<ConsumerInfo>()
        {
            new ConsumerInfo()
            {
                ConsumerType = typeof(FakeConsumer),
                ConsumerOptions = GetFakeConsumerOptions()
            }
        };
    }

    public static ConsumerOptions GetFakeConsumerOptions()
    {
        return new ConsumerOptions()
        {
            Topic = "test",
            ParallelNum = 1
        };
    }

    public static Mock<IServiceProvider> GetMockServiceProviderWithFakeConsumer(bool returnResult)
    {
        var mockServiceProvider = new Mock<IServiceProvider>();
        var mockServiceScope = new Mock<IServiceScope>();
        var mockServiceScopeFactory = new Mock<IServiceScopeFactory>();
        mockServiceScopeFactory.Setup(x => x.CreateScope()).Returns(mockServiceScope.Object);
        // 设置 IServiceProvider 的 GetService 方法返回 IServiceScopeFactory
        mockServiceProvider.Setup(x => x.GetService(typeof(IServiceScopeFactory)))
            .Returns(mockServiceScopeFactory.Object);
        // 设置 IServiceScope 的 ServiceProvider 返回模拟的 IServiceProvider
        mockServiceScope.Setup(x => x.ServiceProvider).Returns(mockServiceProvider.Object);
        mockServiceProvider.Setup(sp => sp.GetService(typeof(FakeConsumer)))
            .Returns(new FakeConsumer(){ReturnResult = returnResult});
        return mockServiceProvider;
    }
}