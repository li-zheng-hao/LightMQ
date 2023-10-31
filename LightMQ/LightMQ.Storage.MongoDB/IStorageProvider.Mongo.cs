using LightMQ.Options;
using LightMQ.Transport;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace LightMQ.Storage.MongoDB;

public class MongoStorageProvider:IStorageProvider
{
    private readonly IMongoClient _mongoClient;
    private readonly IOptions<LightMQOptions> _mqOptions;
    private readonly IOptions<MongoDBOptions> _mongoOptions;

    public MongoStorageProvider(IOptions<LightMQOptions> mqOptions,IOptions<MongoDBOptions> mongoOptions)
    {
        _mqOptions = mqOptions;
        _mongoOptions = mongoOptions;
        _mongoClient=new MongoClient(_mongoOptions.Value.ConnectionString);
    }
    public Task PublishNewMessageAsync(Message message,
        CancellationToken cancellationToken = default)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertOneAsync(message, cancellationToken: cancellationToken);
    }

    public async Task PublishNewMessageAsync(Message message, object transaction,
        CancellationToken cancellationToken = default)
    {
        var client= transaction as IClientSessionHandle;
        await client.Client.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertOneAsync(message, cancellationToken: cancellationToken);
    }

    public Task ClearOldMessagesAsync(CancellationToken cancellationToken = default)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .DeleteManyAsync(it => it.CreateTime <= DateTime.Now.Subtract(_mqOptions.Value.MessageExpireDuration),
                cancellationToken: cancellationToken);
    }

    public Task NackMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateOneAsync(it => it.Id == message.Id,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Waiting),
                cancellationToken: cancellationToken);

    }

    public Task ResetOutOfDateMessagesAsync(CancellationToken cancellationToken = default)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateManyAsync(it => it.CreateTime <= DateTime.Now.Subtract(_mqOptions.Value.MessageTimeoutDuration),
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Waiting),
                cancellationToken: cancellationToken);
    }

    public Task<Message?> PollNewMessageAsync(string topic, CancellationToken cancellationToken = default)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .FindOneAndUpdateAsync(it => it.Topic == topic && it.Status == MessageStatus.Waiting,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Processing),
                cancellationToken: cancellationToken);
    }

    public Task AckMessageAsync(Message currentMessage, CancellationToken stoppingToken = default)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .UpdateOneAsync( it => it.Id == currentMessage.Id,
                Builders<Message>.Update.Set(it => it.Status, MessageStatus.Success),
                cancellationToken: stoppingToken);
    }

    public async Task InitTables(CancellationToken stoppingToken = default)
    {
        var db=_mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName);
        var names =
            (await db.ListCollectionNamesAsync(cancellationToken: stoppingToken).ConfigureAwait(false))
            .ToList();

        if (names.All(n => n != _mqOptions.Value.TableName))
            await db.CreateCollectionAsync(_mqOptions.Value.TableName, cancellationToken: stoppingToken)
                .ConfigureAwait(false);
    }

    public Task PublishNewMessagesAsync(List<Message> messages)
    {
        return _mongoClient.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertManyAsync(messages);
    }

    public Task PublishNewMessagesAsync(List<Message> messages, object transaction)
    {
        var client= transaction as IClientSessionHandle;
        return client.Client.GetDatabase(_mongoOptions.Value.DatabaseName)
            .GetCollection<Message>(_mqOptions.Value.TableName)
            .InsertManyAsync(messages);
    }
}