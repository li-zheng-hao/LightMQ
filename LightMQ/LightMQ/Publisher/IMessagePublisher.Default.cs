using LightMQ.Publisher;
using LightMQ.Transport;
using Newtonsoft.Json;

namespace LightMQ.Storage.MongoDB.MongoMQ.Publisher;

public class MessagePublisher:IMessagePublisher
{
    private readonly IStorageProvider _storageProvider;

    public MessagePublisher(IStorageProvider storageProvider)
    {
        _storageProvider = storageProvider;
    }

    public Task PublishAsync<T>(string topic,T message) where T : class
    {
        var msg=new Message()
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = JsonConvert.SerializeObject(message),
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
        };
        return _storageProvider.PublishNewMessageAsync(msg);
    }

    public Task PublishAsync<T>(string topic, T message, object transaction) where T : class
    {
        var msg=new Message()
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = JsonConvert.SerializeObject(message),
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
        };
        return _storageProvider.PublishNewMessageAsync(msg,transaction);
    }

    public Task PublishAsync<T>(string topic, List<T> message) where T : class
    {
        List<Message> messages = new();
        foreach (var item in message)
        {
            messages.Add(new Message()
            {
                Id = Guid.NewGuid().ToString(),
                Topic = topic,
                Data = JsonConvert.SerializeObject(item),
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
            });
        }
        return _storageProvider.PublishNewMessagesAsync(messages);
    }

    public Task PublishAsync<T>(string topic, List<T> message, object transaction) where T : class
    {
        List<Message> messages = new();
        foreach (var item in message)
        {
            messages.Add(new Message()
            {
                Id = Guid.NewGuid().ToString(),
                Topic = topic,
                Data = JsonConvert.SerializeObject(item),
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
            });
        }
        return _storageProvider.PublishNewMessagesAsync(messages,transaction);
    }
}