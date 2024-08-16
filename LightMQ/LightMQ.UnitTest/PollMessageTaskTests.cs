using Moq;
using Xunit;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using System.Threading.Tasks;
using LightMQ.Consumer;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.UnitTest;
using LightMQ.UnitTest.InternalHelpers;
using LightMQ.Transport;

public class PollMessageTaskTests
{
    private readonly Mock<ILogger<PollMessageTask>> _mockLogger;
    private readonly Mock<IStorageProvider> _mockStorageProvider;
    private readonly Mock<IMessageConsumer> _mockConsumer;
    private readonly PollMessageTask _pollMessageTask;
    private readonly Mock<IServiceProvider> _mockServiceProvider;

    public PollMessageTaskTests()
    {
        _mockLogger = new Mock<ILogger<PollMessageTask>>();
        _mockStorageProvider = new Mock<IStorageProvider>();
        _mockConsumer = new Mock<IMessageConsumer>();
        _mockServiceProvider=MockHelper.GetMockServiceProviderWithFakeConsumer(true);

        _pollMessageTask = new PollMessageTask(_mockLogger.Object,_mockServiceProvider.Object, _mockStorageProvider.Object);
    }

    [Fact]
    public async Task RunAsync_PickRandomQueueMessage_WhenMultiQueue()
    {
        // Arrange
        var consumerInfo = new ConsumerInfo
        {
            ConsumerOptions = new ConsumerOptions
            {
                Topic = "test-topic",
                PollInterval = TimeSpan.FromSeconds(0.1),
                RetryCount = 3,
                RetryInterval = TimeSpan.FromSeconds(1),
                EnableRandomQueue = true
            },
            ConsumerType = typeof(FakeConsumer)
        };
        var message = new Message
        {
            Id = "1",
            Topic = "test-topic",
            Data = "test data",
            Status = MessageStatus.Waiting,
            RetryCount = 0,
            Queue = "queue1"
        };
        var message2 = new Message
        {
            Id = "2",
            Topic = "test-topic",
            Data = "test data2",
            Status = MessageStatus.Waiting,
            RetryCount = 0,
            Queue = "queue2"
        };

        _mockStorageProvider.SetupSequence(sp =>
                sp.PollNewMessageAsync(consumerInfo.ConsumerOptions.Topic,"queue1", It.IsAny<CancellationToken>()))
            .ReturnsAsync(message)
            .ReturnsAsync((Message?)null)
        ;
        _mockStorageProvider.SetupSequence(sp =>
                sp.PollNewMessageAsync(consumerInfo.ConsumerOptions.Topic,"queue2", It.IsAny<CancellationToken>()))
            .ReturnsAsync(message2)
            .ReturnsAsync((Message?)null)
            ;
        _mockStorageProvider.Setup(it=>it.PollAllQueuesAsync(consumerInfo.ConsumerOptions.Topic, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<string> { "queue1","queue2" });
        
        _mockServiceProvider.Setup(sp => sp.GetService(typeof(FakeConsumer)))
            .Returns(new FakeConsumer(){ReturnResult = true});
        
        _mockConsumer.Setup(c => c.ConsumeAsync(message.Data, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        
        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(500); // 设置取消时间

        // Act
        await _pollMessageTask.RunAsync(consumerInfo, cancellationTokenSource.Token);
        
        // Assert
        _mockStorageProvider.Verify(sp => sp.AckMessageAsync(message, It.IsAny<CancellationToken>()), Times.Once);
        _mockStorageProvider.Verify(sp => sp.AckMessageAsync(message2, It.IsAny<CancellationToken>()), Times.Once);
    }
    [Fact]
    public async Task RunAsync_ShouldRetryMessage_WhenConsumerReturnFalse()
    {
        // Arrange
        var consumerInfo = new ConsumerInfo
        {
            ConsumerOptions = new ConsumerOptions
            {
                Topic = "test-topic",
                PollInterval = TimeSpan.FromSeconds(0.1),
                RetryCount = 3,
                RetryInterval = TimeSpan.FromSeconds(1)
            },
            ConsumerType = typeof(FakeConsumer)
        };

        var message = new Message
        {
            Id = "1",
            Topic = "test-topic",
            Data = "test data",
            Status = MessageStatus.Waiting,
            RetryCount = 0
        };

        _mockStorageProvider.SetupSequence(sp =>
                sp.PollNewMessageAsync(consumerInfo.ConsumerOptions.Topic, It.IsAny<CancellationToken>()))
            .ReturnsAsync(message)
            .ReturnsAsync((Message?)null);
        ;
        _mockServiceProvider.Setup(sp => sp.GetService(typeof(FakeConsumer)))
            .Returns(new FakeConsumer(){ReturnResult = false});
        
        _mockConsumer.Setup(c => c.ConsumeAsync(message.Data, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(100); // 设置取消时间

        // Act
        await _pollMessageTask.RunAsync(consumerInfo, cancellationTokenSource.Token);

        // Assert
        _mockStorageProvider.Verify(sp => sp.UpdateRetryInfoAsync(message, It.IsAny<CancellationToken>()), Times.Once);
    }
    [Fact]
    public async Task RunAsync_ShouldConsumeMessage_WhenMessageIsAvailable()
    {
        // Arrange
        var consumerInfo = new ConsumerInfo
        {
            ConsumerOptions = new ConsumerOptions
            {
                Topic = "test-topic",
                PollInterval = TimeSpan.FromSeconds(0.1),
                RetryCount = 3,
                RetryInterval = TimeSpan.FromSeconds(1)
            },
            ConsumerType = typeof(FakeConsumer)
        };

        var message = new Message
        {
            Id = "1",
            Topic = "test-topic",
            Data = "test data",
            Status = MessageStatus.Waiting,
            RetryCount = 0
        };

        _mockStorageProvider.SetupSequence(sp =>
                sp.PollNewMessageAsync(consumerInfo.ConsumerOptions.Topic, It.IsAny<CancellationToken>()))
            .ReturnsAsync(message)
            .ReturnsAsync((Message?)null);
            ;
        _mockConsumer.Setup(c => c.ConsumeAsync(message.Data, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(100); // 设置取消时间

        // Act
        await _pollMessageTask.RunAsync(consumerInfo, cancellationTokenSource.Token);

        // Assert
        _mockStorageProvider.Verify(sp => sp.AckMessageAsync(message, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RunAsync_ShouldHandleException_WhenPollingMessageFails()
    {
        // Arrange
        var consumerInfo = new ConsumerInfo
        {
            ConsumerOptions = new ConsumerOptions
            {
                Topic = "test-topic",
                PollInterval = TimeSpan.FromSeconds(1),
                RetryCount = 3,
                RetryInterval = TimeSpan.FromSeconds(1)
            },
            ConsumerType = typeof(FakeConsumer)

        };

        _mockStorageProvider.Setup(sp => sp.PollNewMessageAsync(consumerInfo.ConsumerOptions.Topic, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new System.Exception("Polling error"));

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(100); // 设置取消时间

        // Act
        await _pollMessageTask.RunAsync(consumerInfo, cancellationTokenSource.Token);

        // Assert
        _mockLogger.VerifyLogging("拉取消息出现异常", LogLevel.Error);
    }

    [Fact]
    public async Task RunAsync_ShouldResetMessageStatus_WhenCancelled()
    {
        // Arrange
        var consumerInfo = new ConsumerInfo
        {
            ConsumerOptions = new ConsumerOptions
            {
                Topic = "test-topic",
                PollInterval = TimeSpan.FromSeconds(0.1),
                RetryCount = 3,
                RetryInterval = TimeSpan.FromSeconds(1)
            },
            ConsumerType = typeof(FakeConsumer)

        };

        var message = new Message
        {
            Id = "1",
            Topic = "test-topic",
            Data = "test data",
            Status = MessageStatus.Processing,
            RetryCount = 0
        };
        _mockServiceProvider.Setup(sp => sp.GetService(typeof(FakeConsumer)))
            .Returns(new FakeConsumer(){ReturnResult = true,Seconds = 5});
        
        _mockStorageProvider.SetupSequence(sp => sp.PollNewMessageAsync(consumerInfo.ConsumerOptions.Topic, It.IsAny<CancellationToken>()))
            .ReturnsAsync(message)
            .ReturnsAsync((Message?)null)
            ;

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(100); // 设置较短的取消时间

        // Act
        await _pollMessageTask.RunAsync(consumerInfo, cancellationTokenSource.Token);

        // Assert
        _mockStorageProvider.Verify(sp => sp.ResetMessageAsync(message,It.IsAny<CancellationToken>()), Times.Once);
    }
}