using LightMQ.Publisher;
using LightMQ.Transport;
using Newtonsoft.Json;

namespace LightMQ.Storage.MongoDB.MongoMQ.Publisher;

public class MongoMessagePublisher:IMessagePublisher
{
    public async Task PublishAsync<T>(string topic,T message) where T : class
    {
        var msg=new Message()
        {
            MessageId = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = JsonConvert.SerializeObject(message),
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
        };
        await msg.SaveAsync();
    }
    
}