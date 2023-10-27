using LightMQ.Options;
using LightMQ.Publisher;
using LightMQ.Storage.MongoDB.MongoMQ.Publisher;
using Microsoft.Extensions.DependencyInjection;

namespace SW.Core.MongoMQ;

public class MongoExtension:IExtension
{
    public IServiceCollection AddExtension(IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<IMessagePublisher, MongoMessagePublisher>();
        return serviceCollection;
    }
}