using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Publisher;
using LightMQ.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;

namespace LightMQ.UnitTest;

public class ServiceCollectionExtensionTests
{
    [Fact]
    public void AddLightMQ_ShouldRegisterServices()
    {
        // Arrange
        IServiceCollection services = new ServiceCollection();
        var options = new LightMQOptions
        {
            Extensions = new List<IExtension>
            {
                new Mock<IExtension>().Object // 使用 Mock 对象
            }
        };

        // 配置委托
        Action<LightMQOptions> configure = opts =>
        {
            opts.Extensions = options.Extensions;
        };

        // Act
        services.AddLightMQ(configure);
        services.AddLogging();
        var mockStorageProvider =new Mock<IStorageProvider>();
        services.AddSingleton(mockStorageProvider.Object);
        var serviceProvider = services!.BuildServiceProvider();

        // Assert
        Assert.NotNull(serviceProvider.GetRequiredService<IMessagePublisher>());
        Assert.NotNull(serviceProvider.GetRequiredService<IConsumerProvider>());
        Assert.NotNull(serviceProvider.GetRequiredService<IPollMessageTask>());
        
        // 验证扩展的 AddExtension 方法被调用
        var extensionMock = Mock.Get(options.Extensions[0]);
        extensionMock.Verify(ext => ext.AddExtension(services), Times.Once);
    }

    [Fact]
    public void AddLightMQ_ShouldConfigureOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        var options = new LightMQOptions();
        
        Action<LightMQOptions> configure = opts =>
        {
            opts.Extensions = new List<IExtension>();
        };

        // Act
        services.AddLightMQ(configure);
        var serviceProvider = services.BuildServiceProvider();
        var optionsMonitor = serviceProvider.GetRequiredService<IOptions<LightMQOptions>>();

        // Assert
        Assert.NotNull(optionsMonitor.Value);
        Assert.Empty(optionsMonitor.Value.Extensions);
    }
}