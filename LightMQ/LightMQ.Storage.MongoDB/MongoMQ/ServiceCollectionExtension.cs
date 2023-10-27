using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;
using SW.Core.MongoMQ;

namespace LightMQ.Storage.MongoDB.MongoMQ;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="serviceCollection"></param>
    /// <returns></returns>
    public static LightMQOptions UseMongoDB(this LightMQOptions mqOptions)
    {
        mqOptions.Extensions.Add(new MongoExtension());
        return mqOptions;
    }
}