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
        string data = message as string ?? JsonConvert.SerializeObject(message);

        var msg=new Message()
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = data,
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            RetryCount = 0,
            ExecutableTime = DateTime.Now
        };
        return _storageProvider.PublishNewMessageAsync(msg);
    }

    public Task PublishAsync<T>(string topic, T message, object transaction) where T : class
    {
        string data = message as string ?? JsonConvert.SerializeObject(message);

        var msg=new Message()
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = data,
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            RetryCount = 0,
            ExecutableTime = DateTime.Now
        };
        return _storageProvider.PublishNewMessageAsync(msg,transaction);
    }

    public Task PublishAsync<T>(string topic, List<T> message) where T : class
    {
        List<Message> messages = new();
        foreach (var item in message)
        {
            string data = item as string ?? JsonConvert.SerializeObject(item);
            messages.Add(new Message()
            {
                Id = Guid.NewGuid().ToString(),
                Topic = topic,
                Data = data,
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                RetryCount = 0,
                ExecutableTime = DateTime.Now
            });
        }
        return _storageProvider.PublishNewMessagesAsync(messages);
    }

    public Task PublishAsync<T>(string topic, List<T> message, object transaction) where T : class
    {
        List<Message> messages = new();
        foreach (var item in message)
        {
            string data = item as string ?? JsonConvert.SerializeObject(item);
            messages.Add(new Message()
            {
                Id = Guid.NewGuid().ToString(),
                Topic = topic,
                Data = data,
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                RetryCount = 0,
                ExecutableTime = DateTime.Now
            });
        }
        return _storageProvider.PublishNewMessagesAsync(messages,transaction);
    }
}