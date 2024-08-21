using Moq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using LightMQ.Publisher;
using LightMQ.Storage;
using LightMQ.Transport;
using Xunit;

public class MessagePublisherTests
{
    private readonly Mock<IStorageProvider> _mockStorageProvider;
    private readonly MessagePublisher _messagePublisher;

    public MessagePublisherTests()
    {
        _mockStorageProvider = new Mock<IStorageProvider>();
        _messagePublisher = new MessagePublisher(_mockStorageProvider.Object);
    }

    [Fact]
    public async Task PublishAsync_ShouldPublishSingleMessageWithQueue()
    {
        // Arrange
        var topic = "test-topic";
        var message = "test message";
        var queue = "test-queue";
        // Act
        await _messagePublisher.PublishAsync(topic, message, queue);

        // Assert
        _mockStorageProvider.Verify(sp => sp.PublishNewMessageAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }
    [Fact]
    public async Task PublishAsync_ShouldPublishSingleMessage()
    {
        // Arrange
        var topic = "test-topic";
        var message = "test message";

        // Act
        await _messagePublisher.PublishAsync(topic, message);

        // Assert
        _mockStorageProvider.Verify(sp => sp.PublishNewMessageAsync(It.IsAny<Message>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }
    [Fact]
    public async Task PublishAsync_WithTransactionAndQueue_ShouldPublishSingleMessage()
    {
        // Arrange
        var topic = "test-topic";
        var message = "test message";
        var transaction = new object();
        var queue = "test-queue";

        // Act
        await _messagePublisher.PublishAsync(topic, message, transaction,queue);

        // Assert
        _mockStorageProvider.Verify(
            sp => sp.PublishNewMessageAsync(It.IsAny<Message>(), transaction, It.IsAny<CancellationToken>())
            , Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WithTransaction_ShouldPublishSingleMessage()
    {
        // Arrange
        var topic = "test-topic";
        var message = "test message";
        var transaction = new object();

        // Act
        await _messagePublisher.PublishAsync(topic, message, transaction);

        // Assert
        _mockStorageProvider.Verify(
            sp => sp.PublishNewMessageAsync(It.IsAny<Message>(), transaction, It.IsAny<CancellationToken>())
            , Times.Once);
    }

    [Fact]
    public async Task PublishAsync_ShouldPublishMultipleMessages()
    {
        // Arrange
        var topic = "test-topic";
        var messages = new List<string> { "message1", "message2", "message3" };

        // Act
        await _messagePublisher.PublishAsync(topic, messages);

        // Assert
        _mockStorageProvider.Verify(sp => sp.PublishNewMessagesAsync(It.IsAny<List<Message>>()), Times.Once);
    }

    [Fact]
    public async Task PublishAsync_WithTransaction_ShouldPublishMultipleMessages()
    {
        // Arrange
        var topic = "test-topic";
        var messages = new List<string> { "message1", "message2", "message3" };
        var transaction = new object();

        // Act
        await _messagePublisher.PublishAsync(topic, messages, transaction);

        // Assert
        _mockStorageProvider.Verify(sp => sp.PublishNewMessagesAsync(It.IsAny<List<Message>>(), transaction),
            Times.Once);
    }

    [Fact]
    public async Task PublishAsync_ShouldSerializeMessageToJson()
    {
        // Arrange
        var topic = "test-topic";
        var message = new { Text = "test message" };

        // Act
        await _messagePublisher.PublishAsync(topic, message);

        // Assert
        _mockStorageProvider.Verify(sp =>
                sp.PublishNewMessageAsync(It.Is<Message>(m => m.Data == JsonConvert.SerializeObject(message)),
                    It.IsAny<CancellationToken>())
            , Times.Once);
    }
}