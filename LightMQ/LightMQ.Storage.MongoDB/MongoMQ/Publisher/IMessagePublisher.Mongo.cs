using LightMQ.Publisher;
using LightMQ.Storage.MongoDB.MongoMQ.Message;
using LightMQ.Transport;
using MongoDB.Entities;
using Newtonsoft.Json;

namespace LightMQ.Storage.MongoDB.MongoMQ.Publisher;

public class MongoMessagePublisher:IMessagePublisher
{
    public async Task PublishAsync<T>(string topic,T message) where T : class
    {
        var msg=new MongoMessage()
        {
            Topic = topic,
            Data = JsonConvert.SerializeObject(message),
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
        };
        msg.MessageId = msg.ID;
        await msg.SaveAsync();
    }
    
}