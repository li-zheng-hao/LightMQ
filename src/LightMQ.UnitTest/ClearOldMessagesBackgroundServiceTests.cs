using LightMQ.BackgroundService;
using LightMQ.Options;
using LightMQ.Storage;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace LightMQ.UnitTest;

public class ClearOldMessagesBackgroundServiceTests
{
    private readonly Mock<IStorageProvider> _mockStorageProvider;
    private readonly Mock<ILogger<ClearOldMessagesBackgroundService>> _mockLogger;
    private readonly IOptions<LightMQOptions> _options;
    private readonly ClearOldMessagesBackgroundService _service;

    public ClearOldMessagesBackgroundServiceTests()
    {
        _mockStorageProvider = new Mock<IStorageProvider>();
        _mockLogger = new Mock<ILogger<ClearOldMessagesBackgroundService>>();
        _options = Microsoft.Extensions.Options.Options.Create(new LightMQOptions { MessageExpireDuration = TimeSpan.FromSeconds(100) });
        _service = new ClearOldMessagesBackgroundService(_mockLogger.Object, _mockStorageProvider.Object, _options);
    }
    [Fact]
    public async Task ExecuteAsync_ShouldClearOldMessages_WhenNotCancelled()
    {
        // Arrange
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // Act
        var executeTask = _service.ExecuteAsync(cancellationToken);
        // 等待一段时间以确保 ExecuteAsync 被调用
        await Task.Delay(200);
        cancellationTokenSource.Cancel();
        await executeTask;

        // Assert
        _mockStorageProvider.Verify(sp => sp.ClearOldMessagesAsync(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        _mockLogger.VerifyLogging("清除历史消息完成");
    }
    [Fact]
    public async Task ExecuteAsync_ShouldNotClearOldMessages_WhenCancelled()
    {
        // Arrange
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;
        cancellationTokenSource.Cancel();
        
        // Act
        var executeTask = _service.ExecuteAsync(cancellationToken);
        // 等待一段时间以确保 ExecuteAsync 被调用
        await executeTask;

        // Assert
        _mockStorageProvider.Verify(sp => sp.ClearOldMessagesAsync(It.IsAny<CancellationToken>()), Times.Never);
        _mockLogger.VerifyLogging("清除历史消息完成",times:Times.Never());

    }
}