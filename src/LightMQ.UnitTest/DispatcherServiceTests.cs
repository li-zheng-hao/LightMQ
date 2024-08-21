using System.Reflection;
using FluentAssertions;
using LightMQ.BackgroundService;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.UnitTest.InternalHelpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace LightMQ.UnitTest;

public class DispatcherServiceTests
{
    private readonly Mock<ILogger<DispatcherService>> _mockLogger;
    private readonly Mock<IServiceProvider> _mockServiceProvider;
    private readonly Mock<IOptions<LightMQOptions>> _mockOptions;
    private readonly DispatcherService _service;
    private readonly Mock<IServiceScope> _mockServiceScope;
    private readonly Mock<IServiceScopeFactory> _mockServiceScopeFactory;
    private readonly Mock<IConsumerProvider> _mockConsumerProvider;
    private readonly Mock<IEnumerable<IBackgroundService>> _mockBackgroundServices;
    private readonly Mock<IStorageProvider> _mockStorageProvider;

    public DispatcherServiceTests()
    {
        _mockLogger = new Mock<ILogger<DispatcherService>>();
        _mockServiceProvider = new Mock<IServiceProvider>();
        _mockStorageProvider = new Mock<IStorageProvider>();
        _mockOptions = new Mock<IOptions<LightMQOptions>>();
        _mockServiceScope = new Mock<IServiceScope>();
        _mockServiceScopeFactory = new Mock<IServiceScopeFactory>();
        _mockServiceScopeFactory.Setup(x => x.CreateScope()).Returns(_mockServiceScope.Object);
        // 设置 IServiceProvider 的 GetService 方法返回 IServiceScopeFactory
        _mockServiceProvider.Setup(x => x.GetService(typeof(IServiceScopeFactory)))
            .Returns(_mockServiceScopeFactory.Object);
        // 设置 IServiceScope 的 ServiceProvider 返回模拟的 IServiceProvider
        _mockServiceScope.Setup(x => x.ServiceProvider).Returns(_mockServiceProvider.Object);
        var options = new LightMQOptions
        {
            ConsumerAssembly = MockHelper.MockAssemblyWithEmpty().Object, // 假设有一个 AssemblyOptions 类
            ExitTimeOut=TimeSpan.FromSeconds(3)
        };

        _mockOptions.Setup(o => o.Value).Returns(options);

        _mockConsumerProvider=new Mock<IConsumerProvider>();
        _mockBackgroundServices=new Mock<IEnumerable<IBackgroundService>>();
        
        _service = new DispatcherService(_mockLogger.Object, _mockServiceProvider.Object,_mockStorageProvider.Object, _mockOptions.Object,_mockConsumerProvider.Object,_mockBackgroundServices.Object);
    }

    [Fact]
    public async Task StartAsync_ShouldLogInformation_WhenServiceStarts()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        // Act
        await _service.StartAsync(cancellationToken);

        // Assert
        _mockLogger.VerifyLogging("LightMQ服务启动", LogLevel.Information);
    }

    [Fact]
    public async Task StartAsync_ShouldLogWarning_WhenNoConsumersFound()
    {
        // Arrange
        var cancellationToken = new CancellationToken();
        _mockConsumerProvider.Setup(it => it.GetConsumerInfos()).Returns(new List<ConsumerInfo>());
        // Act
        await _service.StartAsync(cancellationToken);

        _mockLogger.VerifyLogging("没有扫描到消费者", LogLevel.Information);
    }

    [Fact]
    public async Task StartAsync_ShouldLogError_WhenExceptionOccurs()
    {
        // Arrange
        var cancellationToken = new CancellationToken();
        _mockConsumerProvider.Setup(it => it.GetConsumerInfos()).Throws(new Exception("test exception"));

        // Act
        await _service.StartAsync(cancellationToken);

        // Assert
        _mockLogger.VerifyLogging("LightMQ扫描消费者出现异常", LogLevel.Error, times: Times.Once());
    }

    [Fact]
    public async Task StopAsync_ShouldLogInformation_WhenServiceStops()
    {

        await _service.StartAsync(default);
        // Act
        await _service.StopAsync(default);

        // Assert
        _mockLogger.VerifyLogging("LightMQ服务停止", LogLevel.Information);
    }
    [Fact]
    public async Task StopAsync_ShouldWaitForRunningTasks_ToStop()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        _mockConsumerProvider.Setup(it => it.GetConsumerInfos())
            .Returns(MockHelper.GetFakeConsumerInfos());
        
        var mockPollMessageTask = new Mock<IPollMessageTask>();
        _mockServiceProvider.Setup(sp => sp.GetService(typeof(IPollMessageTask)))
            .Returns(mockPollMessageTask.Object);
        
        await _service.StartAsync(default);
        mockPollMessageTask.Setup(it => it.IsRunning).Returns(true);
        mockPollMessageTask.Setup(it => it.GetConsumerInfo()).Returns(MockHelper.GetFakeConsumerInfos()[0]);
        // Act
        var stopTask = _service.StopAsync(cancellationToken);
        await Task.Delay(200); // 等待一段时间以模拟任务运行
        _mockLogger.VerifyLogging("主题：test的消费者还在运行中，等待结束...",LogLevel.Warning,Times.Once());
        mockPollMessageTask.Setup(it => it.IsRunning).Returns(false);
        await stopTask; // 等待 StopAsync 完成
        _mockLogger.VerifyLogging("LightMQ服务停止", LogLevel.Information);
    }
    [Fact]
    public async Task StopAsync_ShouldCanceledWaitedForRunningTasks_ToStopTimeout()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        _mockConsumerProvider.Setup(it => it.GetConsumerInfos())
            .Returns(MockHelper.GetFakeConsumerInfos());
        
        var mockPollMessageTask = new Mock<IPollMessageTask>();
        _mockServiceProvider.Setup(sp => sp.GetService(typeof(IPollMessageTask)))
            .Returns(mockPollMessageTask.Object);
        
        await _service.StartAsync(default);
        mockPollMessageTask.Setup(it => it.IsRunning).Returns(true);
        mockPollMessageTask.Setup(it => it.GetConsumerInfo()).Returns(MockHelper.GetFakeConsumerInfos()[0]);
        // Act
        await _service.StopAsync(cancellationToken);
        _mockLogger.VerifyLogging("主题：test的消费者还在运行中，等待结束...",LogLevel.Warning,Times.AtLeast(3));
        _mockLogger.VerifyLogging("LightMQ等待退出超时，强制退出", LogLevel.Information);
        _mockLogger.VerifyLogging("LightMQ服务停止", LogLevel.Information);
    }
}