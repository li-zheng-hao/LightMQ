using LightMQ.Options;
using LightMQ.Storage.MongoDB;
using LightMQ.Transport;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace LightMQ.IntegrationTest;

[Collection("Mongo")]
public class MongoRegressionTest:IAsyncLifetime
{
    private readonly MongoStorageProvider _mongoStorageProvider;
    private readonly IMongoDatabase _database;
    private readonly IOptions<LightMQOptions> _options;
    private readonly IOptions<MongoDBOptions> _mongoOptions;
    private readonly IMongoCollection<Message> _collection;

    public MongoRegressionTest()
    {
        _options = Microsoft.Extensions.Options.Options.Create(new LightMQOptions()
        {
        });
        _mongoOptions = Microsoft.Extensions.Options.Options.Create(new MongoDBOptions()
        {
            ConnectionString = Environment.GetEnvironmentVariable("APP_MONGO_CONNECTIONSTRING")!,
            DatabaseName = "LightMQ_IntegrationTest"
        });
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
    public async Task V2_0_0_PollAllQueueAsync_WithNullValue()
    {
        // Arrange
        string topic = "TestTopic";
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "TestData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Queue = null
        };

        // Act
        await _mongoStorageProvider.PublishNewMessageAsync(message);
        var queues=await _mongoStorageProvider.PollAllQueuesAsync(topic);
        
        // Assert
        Assert.True(queues[0]==null);

        var polledMessage = await _mongoStorageProvider.PollNewMessageAsync(topic, queues[0]);
        Assert.NotNull(polledMessage);
    }

}