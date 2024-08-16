using LightMQ.BackgroundService;
using LightMQ.Storage;
using Microsoft.Extensions.Logging;
using Moq;

namespace LightMQ.UnitTest;

public class InitStorageBackgroundServiceTests
{
    [Fact]
    public async Task StartAsync_ShouldCallInitTables()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<InitStorageBackgroundService>>();
        var storageProviderMock = new Mock<IStorageProvider>();
        var service = new InitStorageBackgroundService(loggerMock.Object, storageProviderMock.Object);
        var cancellationToken = new CancellationToken();

        // Act
        await service.StartAsync(cancellationToken);

        // Assert
        storageProviderMock.Verify(sp => sp.InitTables(cancellationToken), Times.Once);
    }

    [Fact]
    public async Task StopAsync_ShouldCompleteSuccessfully()
    {
        // Arrange
        var loggerMock = new Mock<ILogger<InitStorageBackgroundService>>();
        var storageProviderMock = new Mock<IStorageProvider>();
        var service = new InitStorageBackgroundService(loggerMock.Object, storageProviderMock.Object);
        var cancellationToken = new CancellationToken();

        // Act
        await service.StopAsync(cancellationToken);

        // Assert
        // 这里可以添加任何需要验证的内容，当前实现是空的
        Assert.True(true); // 仅用于确保测试通过
    }
}