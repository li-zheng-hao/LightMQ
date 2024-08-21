using Moq;
using Xunit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using LightMQ.Consumer;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.UnitTest.InternalHelpers;

public class ConsumerProviderTests
{
    private readonly Mock<IServiceProvider> _mockServiceProvider;
    private readonly Mock<ILogger<ConsumerProvider>> _mockLogger;
    private readonly Mock<IOptions<LightMQOptions>> _mockOptions;
    private readonly ConsumerProvider _consumerProvider;

    public ConsumerProviderTests()
    {
        _mockLogger = new Mock<ILogger<ConsumerProvider>>();
        _mockOptions = new Mock<IOptions<LightMQOptions>>();

        _mockOptions.Setup(m => m.Value).Returns(new LightMQOptions
        {
            ConsumerAssembly = typeof(FakeConsumer).Assembly // 假设 TestConsumer 是一个实现了 IMessageConsumer 的类
        });

        _mockServiceProvider=MockHelper.GetMockServiceProviderWithFakeConsumer(true);
        _consumerProvider = new ConsumerProvider(_mockServiceProvider.Object, _mockLogger.Object, _mockOptions.Object);
    }

    [Fact]
    public void ScanConsumers_ShouldThrowException_WhenConsumerAssemblyIsNull()
    {
        // Arrange
        _mockOptions.Setup(m => m.Value).Returns(new LightMQOptions { ConsumerAssembly = null });

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => _consumerProvider.ScanConsumers());
        Assert.Equal("Value cannot be null. (Parameter 'ConsumerAssembly')", exception.Message);
    }

    [Fact]
    public void ScanConsumers_ShouldLogWarning_WhenConsumerNotFoundInServiceProvider()
    {
        // Arrange
        var consumerType = typeof(FakeConsumer);
        _mockOptions.Setup(m => m.Value).Returns(new LightMQOptions
        {
            ConsumerAssembly = consumerType.Assembly
        });

        _mockServiceProvider.Setup(sp => sp.GetService(consumerType)).Returns(null); // 模拟未找到服务

        // Act
        _consumerProvider.ScanConsumers();

        // Assert
        _consumerProvider.GetConsumerInfos().Should().BeEmpty();
    }

    [Fact]
    public void ScanConsumers_ShouldAddConsumerInfo_WhenConsumerIsFound()
    {
        // Arrange
        var consumerType = typeof(FakeConsumer);
        var consumerInstance = new FakeConsumer();
        _mockOptions.Setup(m => m.Value).Returns(new LightMQOptions
        {
            ConsumerAssembly = consumerType.Assembly
        });

        _mockServiceProvider.Setup(sp => sp.GetService(consumerType)).Returns(consumerInstance); // 模拟找到服务

        // Act
        _consumerProvider.ScanConsumers();

        // Assert
        var consumerInfos = _consumerProvider.GetConsumerInfos();
        Assert.Single(consumerInfos);
        Assert.Equal(consumerType, consumerInfos[0].ConsumerType);
    }

   
}