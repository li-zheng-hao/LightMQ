﻿using System.Reflection;
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
using Xunit;

namespace LightMQ.UnitTest;

public class DispatcherServiceTests
{
    private readonly Mock<ILogger<DispatcherService>> _mockLogger;
    private readonly Mock<IServiceProvider> _mockServiceProvider;
    private readonly Mock<IOptions<LightMQOptions>> _mockOptions;
    private DispatcherService _service;
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
        _mockServiceProvider
            .Setup(x => x.GetService(typeof(IServiceScopeFactory)))
            .Returns(_mockServiceScopeFactory.Object);
        // 设置 IServiceScope 的 ServiceProvider 返回模拟的 IServiceProvider
        _mockServiceScope.Setup(x => x.ServiceProvider).Returns(_mockServiceProvider.Object);
        var options = new LightMQOptions
        {
            ConsumerAssembly = MockHelper.MockAssemblyWithEmpty().Object, // 假设有一个 AssemblyOptions 类
            ExitTimeOut = TimeSpan.FromSeconds(3),
        };

        _mockOptions.Setup(o => o.Value).Returns(options);

        _mockConsumerProvider = new Mock<IConsumerProvider>();
        _mockBackgroundServices = new Mock<IEnumerable<IBackgroundService>>();

        _service = new DispatcherService(
            _mockLogger.Object,
            _mockServiceProvider.Object,
            _mockStorageProvider.Object,
            _mockOptions.Object,
            _mockConsumerProvider.Object,
            []
        );
    }

    [Fact]
    public async Task StartAsync_ShouldLogInformation_WhenServiceStarts()
    {
        // Arrange
        var cancellationToken = new CancellationToken();
        _mockConsumerProvider.Setup(it => it.GetConsumerInfos()).Returns(new List<ConsumerInfo>());
        // Act
        await _service.StartAsync(cancellationToken);

        await Task.Delay(500);
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
    public async Task StopAsync_ShouldLogInformation_WhenServiceStops()
    {
        _mockConsumerProvider.Setup(it => it.GetConsumerInfos()).Returns(new List<ConsumerInfo>());
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

        _mockConsumerProvider
            .Setup(it => it.GetConsumerInfos())
            .Returns(MockHelper.GetFakeConsumerInfos());

        var mockPollMessageTask = new Mock<IPollMessageTask>();
        _mockServiceProvider
            .Setup(sp => sp.GetService(typeof(IPollMessageTask)))
            .Returns(mockPollMessageTask.Object);

        await _service.StartAsync(default);
        mockPollMessageTask.Setup(it => it.IsRunning).Returns(true);
        mockPollMessageTask
            .Setup(it => it.GetConsumerInfo())
            .Returns(MockHelper.GetFakeConsumerInfos()[0]);
        // Act
        var stopTask = _service.StopAsync(cancellationToken);
        await Task.Delay(200); // 等待一段时间以模拟任务运行
        _mockLogger.VerifyLogging(
            "主题：test的消费者还在运行中，等待结束...",
            LogLevel.Warning,
            Times.Once()
        );
        mockPollMessageTask.Setup(it => it.IsRunning).Returns(false);
        await stopTask; // 等待 StopAsync 完成
        _mockLogger.VerifyLogging("LightMQ服务停止", LogLevel.Information);
    }

    [Fact]
    public async Task StopAsync_ShouldCanceledWaitedForRunningTasks_ToStopTimeout()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        _mockConsumerProvider
            .Setup(it => it.GetConsumerInfos())
            .Returns(MockHelper.GetFakeConsumerInfos());

        var mockPollMessageTask = new Mock<IPollMessageTask>();
        _mockServiceProvider
            .Setup(sp => sp.GetService(typeof(IPollMessageTask)))
            .Returns(mockPollMessageTask.Object);

        mockPollMessageTask.Setup(it => it.IsRunning).Returns(true);
        mockPollMessageTask
            .Setup(it => it.GetConsumerInfo())
            .Returns(MockHelper.GetFakeConsumerInfos()[0]);

        await _service.StartAsync(default);
        await Task.Delay(1000);
        // Act
        await _service.StopAsync(cancellationToken);
        _mockLogger.VerifyLogging(
            "主题：test的消费者还在运行中，等待结束...",
            LogLevel.Warning,
            Times.AtLeast(3)
        );
        _mockLogger.VerifyLogging("LightMQ等待退出超时，强制退出", LogLevel.Information);
        _mockLogger.VerifyLogging("LightMQ服务停止", LogLevel.Information);
    }

    [Fact]
    public async Task StartAsync_ShouldNotBlock_WhenScanConsumersIsSlow()
    {
        // Arrange
        var cancellationToken = new CancellationTokenSource();
        var scanConsumersCalled = false;
        _mockConsumerProvider
            .Setup(it => it.ScanConsumers())
            .Callback(() =>
            {
                scanConsumersCalled = true;
                Thread.Sleep(2000); // 模拟耗时
            });
        _mockConsumerProvider.Setup(it => it.GetConsumerInfos()).Returns(new List<ConsumerInfo>());

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await _service.StartAsync(cancellationToken.Token);

        stopwatch.Stop();

        stopwatch.ElapsedMilliseconds.Should().BeLessThan(100, "StartAsync应快速返回");
        scanConsumersCalled.Should().BeTrue();
        cancellationToken.Cancel();
    }

    [Fact]
    public void DispatcherService_ShouldAllowReassignment_WhenServiceFieldIsNotReadonly()
    {
        // Arrange - 验证_service字段可以重新赋值（不再是readonly）
        var originalService = _service;

        // Act - 重新创建服务实例
        _service = new DispatcherService(
            _mockLogger.Object,
            _mockServiceProvider.Object,
            _mockStorageProvider.Object,
            _mockOptions.Object,
            _mockConsumerProvider.Object,
            []
        );

        // Assert - 验证可以成功重新赋值
        _service.Should().NotBeSameAs(originalService);
        _service.Should().NotBeNull();
    }

    [Fact]
    public async Task StopAsync_ShouldWaitAfterStart_WhenTestingServiceLifecycle()
    {
        // Arrange
        var cancellationToken = new CancellationToken();

        _mockConsumerProvider
            .Setup(it => it.GetConsumerInfos())
            .Returns(MockHelper.GetFakeConsumerInfos());

        var mockPollMessageTask = new Mock<IPollMessageTask>();
        _mockServiceProvider
            .Setup(sp => sp.GetService(typeof(IPollMessageTask)))
            .Returns(mockPollMessageTask.Object);

        mockPollMessageTask.Setup(it => it.IsRunning).Returns(true);
        mockPollMessageTask
            .Setup(it => it.GetConsumerInfo())
            .Returns(MockHelper.GetFakeConsumerInfos()[0]);

        // Act - 按照修改后的顺序：先启动，然后等待，再停止
        await _service.StartAsync(default);
        await Task.Delay(1000); // 等待服务启动完成
        await _service.StopAsync(cancellationToken);

        // Assert - 验证服务正常启动和停止
        _mockLogger.VerifyLogging("LightMQ服务启动", LogLevel.Information);
        _mockLogger.VerifyLogging("LightMQ服务停止", LogLevel.Information);
    }
}
