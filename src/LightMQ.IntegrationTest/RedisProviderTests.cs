using LightMQ.Options;
using LightMQ.Storage.Redis;
using LightMQ.Transport;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace LightMQ.IntegrationTest;

public class RedisProviderTests : IAsyncLifetime
{
    private readonly RedisStorageProvider _provider;
    private readonly IDatabase _db;
    private readonly IOptions<LightMQOptions> _options;
    private readonly IOptions<RedisOptions> _redisOptions;

    public RedisProviderTests()
    {
        _options = Microsoft.Extensions.Options.Options.Create(new LightMQOptions());
        var connStr = Environment.GetEnvironmentVariable("APP_REDIS_CONNECTIONSTRING") ?? "localhost:6379";
        _redisOptions = Microsoft.Extensions.Options.Options.Create(
            new RedisOptions { ConnectionString = connStr }
        );
        _provider = new RedisStorageProvider(_options, _redisOptions);
        _db = ConnectionMultiplexer.Connect(connStr).GetDatabase();
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        var prefix = _redisOptions.Value.KeyPrefix;
        var server = _db.Multiplexer.GetServer(_redisOptions.Value.ConnectionString);
        foreach (var key in server.Keys(pattern: $"{prefix}:*"))
            await _db.KeyDeleteAsync(key);
    }

    [Fact]
    public async Task PublishNewMessageAsync_ShouldInsertMessage()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic", Data = "TestData",
            CreateTime = DateTime.Now, Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now, RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);

        var exists = await _db.KeyExistsAsync($"{_redisOptions.Value.KeyPrefix}:TestTopic:msg:{msg.Id}");
        Assert.True(exists);
    }

    [Fact]
    public async Task PublishNewMessageAsync_WithQueue_ShouldAddToQueueSet()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "QT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0, Queue = "q1",
        };

        await _provider.PublishNewMessageAsync(msg);

        var score = await _db.SortedSetScoreAsync($"{_redisOptions.Value.KeyPrefix}:QT:pending:q:q1", msg.Id);
        Assert.NotNull(score);
    }

    [Fact]
    public async Task PollNewMessageAsync_ShouldReturnAndMarkProcessing()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "PollT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("PollT");
        Assert.NotNull(polled);
        Assert.Equal(msg.Id, polled.Id);
        Assert.Equal(MessageStatus.Processing, polled.Status);
    }

    [Fact]
    public async Task PollNewMessageAsync_WithQueue_ShouldReturnFromCorrectQueue()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "PollQT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0, Queue = "myq",
        };

        await _provider.PublishNewMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("PollQT", "myq");
        Assert.NotNull(polled);
        Assert.Equal(msg.Id, polled.Id);
    }

    [Fact]
    public async Task PollNewMessageAsync_ShouldReturnNull_WhenNoMessages()
    {
        var polled = await _provider.PollNewMessageAsync("NoExist");
        Assert.Null(polled);
    }

    [Fact]
    public async Task PollNewMessageAsync_ShouldReturnNull_WhenNotYetExecutable()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "FutureT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddHours(1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("FutureT");
        Assert.Null(polled);
    }

    [Fact]
    public async Task AckMessageAsync_ShouldRemoveFromProcessing()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "AckT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);
        await _provider.PollNewMessageAsync("AckT");
        await _provider.AckMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("AckT");
        Assert.Null(polled);
    }

    [Fact]
    public async Task NackMessageAsync_ShouldRemoveFromProcessing()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "NackT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);
        await _provider.PollNewMessageAsync("NackT");
        await _provider.NackMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("NackT");
        Assert.Null(polled);
    }

    [Fact]
    public async Task RequeueMessageAsync_ShouldBePollableAgain()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "RequeueT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);
        await _provider.PollNewMessageAsync("RequeueT");
        await _provider.RequeueMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("RequeueT");
        Assert.NotNull(polled);
        Assert.Equal(msg.Id, polled.Id);
    }

    [Fact]
    public async Task UpdateRetryInfoAsync_ShouldUpdateRetryCount()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "RetryT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);
        await _provider.PollNewMessageAsync("RetryT");

        msg.RetryCount = 1;
        msg.ExecutableTime = DateTime.Now.AddMinutes(-1);
        await _provider.UpdateRetryInfoAsync(msg);

        var polled = await _provider.PollNewMessageAsync("RetryT");
        Assert.NotNull(polled);
        Assert.Equal(1, polled.RetryCount);
    }

    [Fact]
    public async Task ResetMessageAsync_ShouldMakePollableAgain()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "ResetT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Failed, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);
        await _provider.ResetMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("ResetT");
        Assert.NotNull(polled);
        Assert.Equal(msg.Id, polled.Id);
    }

    [Fact]
    public async Task ResetOutOfDateMessagesAsync_ShouldResetExpiredProcessing()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "TimeoutT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-2),
            RetryCount = 0,
        };

        await _provider.PublishNewMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("TimeoutT");
        Assert.NotNull(polled);

        await _provider.ResetOutOfDateMessagesAsync("TimeoutT", DateTime.Now);

        var polledAgain = await _provider.PollNewMessageAsync("TimeoutT");
        Assert.NotNull(polledAgain);
        Assert.Equal(msg.Id, polledAgain.Id);
    }

    [Fact]
    public async Task PollAllQueuesAsync_ShouldReturnDistinctQueues()
    {
        var topic = "MultiQueueT";

        await _provider.PublishNewMessageAsync(new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = topic, Data = "D1",
            CreateTime = DateTime.Now, Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now, RetryCount = 0, Queue = "q1",
        });
        await _provider.PublishNewMessageAsync(new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = topic, Data = "D2",
            CreateTime = DateTime.Now, Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now, RetryCount = 0, Queue = "q2",
        });
        await _provider.PublishNewMessageAsync(new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = topic, Data = "D3",
            CreateTime = DateTime.Now, Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now, RetryCount = 0, Queue = "q1",
        });

        var queues = await _provider.PollAllQueuesAsync(topic);
        Assert.Contains("q1", queues);
        Assert.Contains("q2", queues);
    }

    [Fact]
    public async Task PublishNewMessagesAsync_ShouldInsertMultiple()
    {
        var msgs = new List<Message>
        {
            new() { Id = Guid.NewGuid().ToString(), Topic = "BatchT", Data = "D1",
                CreateTime = DateTime.Now, Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now, RetryCount = 0, Queue = "q1" },
            new() { Id = Guid.NewGuid().ToString(), Topic = "BatchT", Data = "D2",
                CreateTime = DateTime.Now, Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now, RetryCount = 0, Queue = "q2" },
        };

        await _provider.PublishNewMessagesAsync(msgs);

        foreach (var m in msgs)
            Assert.True(await _db.KeyExistsAsync($"{_redisOptions.Value.KeyPrefix}:BatchT:msg:{m.Id}"));
    }

    [Fact]
    public async Task GlobalPoll_ShouldFindQueuedMessages()
    {
        var msg = new Message
        {
            Id = Guid.NewGuid().ToString(), Topic = "GlobalQueueT",
            Data = "D", CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting, ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0, Queue = "somequeue",
        };

        await _provider.PublishNewMessageAsync(msg);

        var polled = await _provider.PollNewMessageAsync("GlobalQueueT");
        Assert.NotNull(polled);
        Assert.Equal(msg.Id, polled.Id);
    }

    [Fact]
    public async Task ClearOldMessagesAsync_ShouldBeNoOp()
    {
        await _provider.ClearOldMessagesAsync();
    }

    [Fact]
    public async Task InitTables_ShouldBeNoOp()
    {
        await _provider.InitTables();
    }
}