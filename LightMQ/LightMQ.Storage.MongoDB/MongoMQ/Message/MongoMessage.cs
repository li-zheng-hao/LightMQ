using LightMQ.Transport;
using MongoDB.Entities;

namespace LightMQ.Storage.MongoDB.MongoMQ.Message;

public class MongoMessage : IMessage, IEntity
{
    public string MessageId { get; set; }
    
    public MessageStatus Status { get; set; }
    
    public string Data { get; set; }
    
    public string Topic { get; set; }
    
    public DateTime CreateTime { get; set; }
    public string GenerateNewID()
    {
        return Guid.NewGuid().ToString();
    }

    public string ID { get; set; }
}