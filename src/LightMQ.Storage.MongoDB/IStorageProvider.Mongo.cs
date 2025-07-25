using LightMQ.Options;
using LightMQ.Transport;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace LightMQ.Storage.MongoDB;

public class MongoStorageProvider : IStorageProvider
{
    private IMongoClient? _mongoClient;
    private object locker = new();
    private readonly IOptions<LightMQOptions> _mqOptions;
    private readonly IOptions<MongoDBOptions> _mongoOptions;

    public MongoStorageProvider(
        IOptions<LightMQOptions> mqOptions,
        IOptions<MongoDBOptions> mongoOptions
    )
    {
        _mqOptions = mqOptions;
        _mongoOptions = mongoOptions;
    }

    private IMongoClient GetMongoClient()
    {
        if (_mongoClient == null)
        {
            lock (locker)
            {
                _mongoClient = new MongoClient(_mongoOptions.Value.ConnectionString);
            }
        }
        return _mongoClient;
    }

    public Task PublishNewMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertOneAsync(message, cancellationToken: cancellationToken);
    }

    public async Task PublishNewMessageAsync(
        Message message,
        object transaction,
        CancellationToken cancellationToken = default
    )
    {
        var client = transaction as IClientSessionHandle;
        await client!
            .Client.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertOneAsync(message, cancellationToken: cancellationToken);
    }

    public Task ClearOldMessagesAsync(CancellationToken cancellationToken = default)
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .DeleteManyAsync(
                it =>
                    it.CreateTime <= DateTime.Now.Subtract(_mqOptions.Value.MessageExpireDuration),
                cancellationToken: cancellationToken
            );
    }

    public Task NackMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateOneAsync(
                it => it.Id == message.Id,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Failed),
                cancellationToken: cancellationToken
            );
    }

    public Task ResetMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateOneAsync(
                it => it.Id == message.Id,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Waiting),
                cancellationToken: cancellationToken
            );
    }

    public Task UpdateRetryInfoAsync(Message message, CancellationToken cancellationToken = default)
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateOneAsync(
                it => it.Id == message.Id,
                Builders<Message>
                    .Update.Set(it => it.RetryCount, message.RetryCount)
                    .Set(it => it.ExecutableTime, message.ExecutableTime)
                    .Set(it => it.Status, MessageStatus.Waiting),
                cancellationToken: cancellationToken
            );
    }

    public Task ResetOutOfDateMessagesAsync(
        string topic,
        DateTime executeTime,
        CancellationToken cancellationToken = default
    )
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateManyAsync(
                it =>
                    it.ExecutableTime <= executeTime
                    && it.Status == MessageStatus.Processing
                    && it.Topic == topic,
                Builders<Message>
                    .Update.Set(it => it.Status, MessageStatus.Waiting)
                    .Set(it => it.ExecutableTime, DateTime.Now),
                cancellationToken: cancellationToken
            );
    }

    public Task<Message?> PollNewMessageAsync(
        string topic,
        CancellationToken cancellationToken = default
    )
    {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .FindOneAndUpdateAsync(
                it =>
                    it.Topic == topic
                    && it.Status == MessageStatus.Waiting
                    && it.ExecutableTime <= DateTime.Now,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Processing),
                cancellationToken: cancellationToken
            );
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
    }

    public Task<Message?> PollNewMessageAsync(
        string topic,
        string? queue,
        CancellationToken cancellationToken = default
    )
    {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .FindOneAndUpdateAsync(
                it =>
                    it.Topic == topic
                    && it.Status == MessageStatus.Waiting
                    && it.ExecutableTime <= DateTime.Now
                    && it.Queue == queue,
                Builders<Message>
                    .Update.Set(it => it.Status, MessageStatus.Processing)
                    .Set(it => it.ExecutableTime, DateTime.Now),
                cancellationToken: cancellationToken
            );
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
    }

    public async Task<List<string?>> PollAllQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default
    )
    {
        var collection = GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName);

        var result = await collection
            .Aggregate()
            .Match(m =>
                m.Topic == topic
                && m.Status == MessageStatus.Waiting
                && m.ExecutableTime <= DateTime.Now
            ) // 过滤条件：Topic 等于传入的 topic
            .Group(
                new BsonDocument
                {
                    { "_id", "$Queue" }, // 根据 Queue 字段进行分组
                    { "Queue", new BsonDocument("$first", "$Queue") }, // 获取每个 Queue 的第一个值
                }
            )
            .Project(
                new BsonDocument
                {
                    { "Queue", "$Queue" }, // 只返回 Queue 字段
                }
            )
            .ToListAsync(cancellationToken);

        // 提取 Queue 字段并转换为 List<string>
        return result
            .Select(r =>
            {
                if (r["Queue"].IsString)
                    return r["Queue"].AsString;
                return null;
            })
            .ToList();
    }

    public Task AckMessageAsync(Message currentMessage, CancellationToken stoppingToken = default)
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateOneAsync(
                it => it.Id == currentMessage.Id,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Success),
                cancellationToken: stoppingToken
            );
    }

    public async Task InitTables(CancellationToken stoppingToken = default)
    {
        var db = GetMongoClient().GetDatabase(_mongoOptions.Value.DatabaseName);
        var names = (
            await db.ListCollectionNamesAsync(cancellationToken: stoppingToken)
                .ConfigureAwait(false)
        ).ToList();

        if (names.All(n => n != _mqOptions.Value.TableName))
            await db.CreateCollectionAsync(
                    _mqOptions.Value.TableName,
                    cancellationToken: stoppingToken
                )
                .ConfigureAwait(false);
    }

    public Task PublishNewMessagesAsync(List<Message> messages)
    {
        return GetMongoClient()
            .GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertManyAsync(messages);
    }

    public Task PublishNewMessagesAsync(List<Message> messages, object transaction)
    {
        var client = transaction as IClientSessionHandle;
        return client!
            .Client.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertManyAsync(messages);
    }
}
