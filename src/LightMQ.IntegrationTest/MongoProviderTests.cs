using LightMQ.Options;
using LightMQ.Storage.MongoDB;
using LightMQ.Transport;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace LightMQ.IntegrationTest;

[Collection("Mongo")]
public class MongoProviderTests : IAsyncLifetime
{
    private readonly MongoStorageProvider _mongoStorageProvider;
    private readonly IMongoDatabase _database;
    private readonly IOptions<LightMQOptions> _options;
    private readonly IOptions<MongoDBOptions> _mongoOptions;
    private readonly IMongoCollection<Message> _collection;

    public MongoProviderTests()
    {
        _options = Microsoft.Extensions.Options.Options.Create(new LightMQOptions() { });
        _mongoOptions = Microsoft.Extensions.Options.Options.Create(
            new MongoDBOptions()
            {
                ConnectionString = Environment.GetEnvironmentVariable(
                    "APP_MONGO_CONNECTIONSTRING"
                )!,
                DatabaseName = "LightMQ_IntegrationTest",
            }
        );
        _mongoStorageProvider = new MongoStorageProvider(_options, _mongoOptions);
        var client = new MongoClient(_mongoOptions.Value.ConnectionString);
        _database = client.GetDatabase(_mongoOptions.Value.DatabaseName);
        _collection = _database.GetCollection<Message>(_options.Value.TableName);
    }

    public async Task InitializeAsync()
    {
        // 在测试开始前清空数据库
        await _database.DropCollectionAsync(_options.Value.TableName);
        await _mongoStorageProvider.InitTables(CancellationToken.None);
    }

    public Task DisposeAsync()
    {
        // 在测试结束后清空数据库
        return _database.DropCollectionAsync(_options.Value.TableName);
    }

    [Fact]
    public async Task PublishNewMessageAsync_ShouldInsertMessage()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.UtcNow,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.UtcNow,
            RetryCount = 0,
            Queue = "TestQueue",
        };

        // Act
        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Assert
        var insertedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.NotNull(insertedMessage);
        Assert.Equal(message.Topic, insertedMessage.Topic);
    }

    [Fact]
    public async Task ClearOldMessagesAsync_ShouldRemoveOldMessages()
    {
        // Arrange
        var oldMessage = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "OldTopic",
            Data = "OldData",
            CreateTime = DateTime.UtcNow.AddDays(-8), // 设置为旧消息
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.UtcNow.AddDays(-1),
            RetryCount = 0,
            Queue = "OldQueue",
        };

        await _mongoStorageProvider.PublishNewMessageAsync(oldMessage, CancellationToken.None);

        // Act
        await _mongoStorageProvider.ClearOldMessagesAsync(CancellationToken.None);

        // Assert
        var deletedMessage = await _collection
            .Find(m => m.Id == oldMessage.Id)
            .FirstOrDefaultAsync();
        Assert.Null(deletedMessage); // 确保旧消息已被删除
    }

    [Fact]
    public async Task PublishNewMessageAsync_WithTransaction_ShouldInsertMessage()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.UtcNow,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.UtcNow,
            RetryCount = 0,
            Queue = "TestQueue",
        };

        using var clientSession = await _database.Client.StartSessionAsync();
        clientSession.StartTransaction();

        // Act
        await _mongoStorageProvider.PublishNewMessageAsync(
            message,
            clientSession,
            CancellationToken.None
        );
        await clientSession.CommitTransactionAsync();

        // Assert
        var insertedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.NotNull(insertedMessage);
        Assert.Equal(message.Topic, insertedMessage.Topic);
    }

    [Fact]
    public async Task NackMessageAsync_ShouldUpdateMessageStatusToFailed()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.UtcNow,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.UtcNow,
            RetryCount = 0,
            Queue = "TestQueue",
        };

        // 插入消息以便后续测试
        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _mongoStorageProvider.NackMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.NotNull(updatedMessage);
        Assert.Equal(MessageStatus.Failed, updatedMessage.Status);
    }

    [Fact]
    public async Task ResetMessageAsync_ShouldUpdateMessageStatusToWaiting()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.UtcNow,
            Status = MessageStatus.Failed, // 初始状态为 Failed
            ExecutableTime = DateTime.UtcNow,
            RetryCount = 0,
        };

        // 插入消息以便后续测试
        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _mongoStorageProvider.ResetMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.NotNull(updatedMessage);
        Assert.Equal(MessageStatus.Waiting, updatedMessage.Status); // 验证状态是否更新为 Waiting
    }

    [Fact]
    public async Task UpdateRetryInfoAsync_ShouldUpdateMessageRetryInfo()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "TestQueue",
        };

        // 插入消息以便后续测试
        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        message.RetryCount = 1; // 更新重试次数
        message.ExecutableTime = DateTime.Now.AddMinutes(10); // 更新可执行时间
        await _mongoStorageProvider.UpdateRetryInfoAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.NotNull(updatedMessage);
        Assert.Equal(1, updatedMessage.RetryCount);
        var span = (message.ExecutableTime - updatedMessage.ExecutableTime.ToLocalTime());
        Assert.True(span < TimeSpan.FromSeconds(0.01));
    }

    [Fact]
    public async Task ResetOutOfDateMessagesAsync_ShouldUpdateOutOfDateMessages()
    {
        // Arrange
        var oldMessage = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "OldTopic",
            Data = "OldData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Processing,
            ExecutableTime = DateTime.Now.AddMinutes(-10), // 设置为过期消息
            RetryCount = 0,
            Queue = "OldQueue",
        };

        await _mongoStorageProvider.PublishNewMessageAsync(oldMessage, CancellationToken.None);

        // Act
        await _mongoStorageProvider.ResetOutOfDateMessagesAsync(
            "OldTopic",
            DateTime.Now,
            CancellationToken.None
        );

        // Assert
        var updatedMessage = await _collection
            .Find(m => m.Id == oldMessage.Id)
            .FirstOrDefaultAsync();
        Assert.NotNull(updatedMessage);
        Assert.Equal(MessageStatus.Waiting, updatedMessage.Status); // 验证状态是否更新为 Waiting
    }

    [Fact]
    public async Task PollNewMessageAsync_ShouldReturnAndUpdateMessageStatus()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "PollTopic",
            Data = "PollData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "PollQueue",
        };

        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        var polledMessage = await _mongoStorageProvider.PollNewMessageAsync(
            "PollTopic",
            CancellationToken.None
        );

        // Assert
        Assert.NotNull(polledMessage);
        Assert.Equal(message.Id, polledMessage.Id);
        var updatedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.Equal(MessageStatus.Processing, updatedMessage.Status); // 验证状态是否更新为 Processing
    }

    [Fact]
    public async Task PollNewMessageAsync_WithQueue_ShouldReturnAndUpdateMessageStatus()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "PollTopic",
            Data = "PollData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "PollQueue",
        };

        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        var polledMessage = await _mongoStorageProvider.PollNewMessageAsync(
            "PollTopic",
            "PollQueue",
            CancellationToken.None
        );

        // Assert
        Assert.NotNull(polledMessage);
        Assert.Equal(message.Id, polledMessage.Id);
        var updatedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.Equal(MessageStatus.Processing, updatedMessage.Status); // 验证状态是否更新为 Processing
    }

    [Fact]
    public async Task AckMessageAsync_ShouldUpdateMessageStatusToSuccess()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "AckTopic",
            Data = "AckData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Processing,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "AckQueue",
        };

        await _mongoStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _mongoStorageProvider.AckMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = await _collection.Find(m => m.Id == message.Id).FirstOrDefaultAsync();
        Assert.NotNull(updatedMessage);
        Assert.Equal(MessageStatus.Success, updatedMessage.Status); // 验证状态是否更新为 Success
    }

    [Fact]
    public async Task PollAllQueuesAsync_ShouldReturnDistinctQueues()
    {
        // Arrange
        var topic = "TestTopic";

        // 插入多个消息到不同的队列
        var message1 = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "Data1",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "Queue1",
        };

        var message2 = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "Data2",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "Queue2",
        };

        var message3 = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "Data3",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = "Queue1", // 重复队列
        };

        await _mongoStorageProvider.PublishNewMessageAsync(message1, CancellationToken.None);
        await _mongoStorageProvider.PublishNewMessageAsync(message2, CancellationToken.None);
        await _mongoStorageProvider.PublishNewMessageAsync(message3, CancellationToken.None);

        // Act
        var queues = await _mongoStorageProvider.PollAllQueuesAsync(topic, CancellationToken.None);

        // Assert
        Assert.NotNull(queues);
        Assert.Equal(2, queues.Count); // 应该返回两个不同的队列
        Assert.Contains("Queue1", queues);
        Assert.Contains("Queue2", queues);
    }

    [Fact]
    public async Task PublishNewMessagesAsync_ShouldInsertMultipleMessages()
    {
        // Arrange
        var messages = new List<Message>
        {
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic1",
                Data = "Data1",
                CreateTime = DateTime.UtcNow,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
                RetryCount = 0,
                Queue = "Queue1",
            },
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic2",
                Data = "Data2",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
                RetryCount = 0,
                Queue = "Queue2",
            },
        };

        // Act
        await _mongoStorageProvider.PublishNewMessagesAsync(messages);

        // Assert
        var insertedMessages = await _collection
            .Find(m => messages.Select(msg => msg.Id).Contains(m.Id))
            .ToListAsync();
        Assert.Equal(messages.Count, insertedMessages.Count); // 验证插入的消息数量
        foreach (var message in messages)
        {
            Assert.Contains(insertedMessages, m => m.Id == message.Id); // 验证每条消息是否存在
        }
    }

    [Fact]
    public async Task PublishNewMessagesAsync_WithTransaction_ShouldInsertMultipleMessages()
    {
        // Arrange
        var messages = new List<Message>
        {
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic1",
                Data = "Data1",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
                RetryCount = 0,
                Queue = "Queue1",
            },
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic2",
                Data = "Data2",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
                RetryCount = 0,
                Queue = "Queue2",
            },
        };

        using var clientSession = await _database.Client.StartSessionAsync();
        clientSession.StartTransaction();

        // Act
        await _mongoStorageProvider.PublishNewMessagesAsync(messages, clientSession);
        await clientSession.CommitTransactionAsync();

        // Assert
        var insertedMessages = await _collection
            .Find(m => messages.Select(msg => msg.Id).Contains(m.Id))
            .ToListAsync();
        Assert.Equal(messages.Count, insertedMessages.Count); // 验证插入的消息数量
        foreach (var message in messages)
        {
            Assert.Contains(insertedMessages, m => m.Id == message.Id); // 验证每条消息是否存在
        }
    }
}
