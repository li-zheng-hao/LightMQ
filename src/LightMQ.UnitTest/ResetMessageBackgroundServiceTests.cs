using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using LightMQ.BackgroundService;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.UnitTest;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

public class ResetMessageBackgroundServiceTests
{
    private readonly Mock<ILogger<ResetMessageBackgroundService>> _mockLogger;
    private readonly Mock<IStorageProvider> _mockStorageProvider;
    private readonly Mock<IOptions<LightMQOptions>> _mockOptions;
    private readonly Mock<IConsumerProvider> _mockConsumerProvider;
    private readonly ResetMessageBackgroundService _service;

    public ResetMessageBackgroundServiceTests()
    {
        _mockLogger = new Mock<ILogger<ResetMessageBackgroundService>>();
        _mockStorageProvider = new Mock<IStorageProvider>();
        _mockOptions = new Mock<IOptions<LightMQOptions>>();
        _mockConsumerProvider = new Mock<IConsumerProvider>();

        var options = new LightMQOptions { };
        _mockOptions.Setup(o => o.Value).Returns(options);

        _service = new ResetMessageBackgroundService(
            _mockLogger.Object,
            _mockStorageProvider.Object,
            _mockOptions.Object,
            _mockConsumerProvider.Object
        );
    }

    [Fact]
    public async Task ExecuteAsync_ShouldResetMessages_WhenNotCancelled()
    {
        // Arrange
        var cancellationToken = new CancellationTokenSource();
        cancellationToken.CancelAfter(500); // 设置取消时间

        var consumerInfo = new ConsumerInfo
        {
            ConsumerOptions = new ConsumerOptions
            {
                Topic = "test-topic",
                ResetInterval = TimeSpan.FromMinutes(1),
            },
        };
        _mockConsumerProvider
            .Setup(cp => cp.GetConsumerInfos())
            .Returns(new List<ConsumerInfo> { consumerInfo });

        _mockStorageProvider
            .Setup(sp =>
                sp.ResetOutOfDateMessagesAsync(
                    It.IsAny<string>(),
                    It.IsAny<DateTime>(),
                    It.IsAny<CancellationToken>()
                ))
            .Returns(Task.CompletedTask);

        // Act
        await _service.ExecuteAsync(cancellationToken.Token);

        // Assert
        _mockStorageProvider.Verify(
            sp =>
                sp.ResetOutOfDateMessagesAsync(
                    consumerInfo.ConsumerOptions.Topic,
                    It.IsAny<DateTime>(),
                    It.IsAny<CancellationToken>()
                ),
            Times.AtLeastOnce
        );
        _mockLogger.VerifyLogging("重置超时消息状态完成");
    }
}
