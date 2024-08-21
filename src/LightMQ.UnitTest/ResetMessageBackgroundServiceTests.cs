using Moq;
using Xunit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Threading;
using System.Threading.Tasks;
using LightMQ.BackgroundService;
using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.UnitTest;

public class ResetMessageBackgroundServiceTests
{
    private readonly Mock<ILogger<ResetMessageBackgroundService>> _mockLogger;
    private readonly Mock<IStorageProvider> _mockStorageProvider;
    private readonly Mock<IOptions<LightMQOptions>> _mockOptions;
    private readonly ResetMessageBackgroundService _service;

    public ResetMessageBackgroundServiceTests()
    {
        _mockLogger = new Mock<ILogger<ResetMessageBackgroundService>>();
        _mockStorageProvider = new Mock<IStorageProvider>();
        _mockOptions = new Mock<IOptions<LightMQOptions>>();

        var options = new LightMQOptions
        {
            MessageTimeoutDuration = TimeSpan.FromSeconds(2) 
        };
        _mockOptions.Setup(o => o.Value).Returns(options);

        _service = new ResetMessageBackgroundService(_mockLogger.Object, _mockStorageProvider.Object, _mockOptions.Object);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldResetMessages_WhenNotCancelled()
    {
        // Arrange
        var cancellationToken = new CancellationTokenSource();
        cancellationToken.CancelAfter(500); // 设置取消时间

        _mockStorageProvider.Setup(sp => sp.ResetOutOfDateMessagesAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        await _service.ExecuteAsync(cancellationToken.Token);

        // Assert
        _mockStorageProvider.Verify(sp => sp.ResetOutOfDateMessagesAsync(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _mockLogger.VerifyLogging("重置超时消息状态完成");
    }
}