using LightMQ.BackgroundService;
using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;

namespace SW.Core.MongoMQ;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="serviceCollection"></param>
    /// <returns></returns>
    public static IServiceCollection AddMongoMQ(this IServiceCollection serviceCollection,Action<LightMQOptions> configure)
    {
        var options = new LightMQOptions();
        configure(options);
        foreach (var extension in options.Extensions)
        {
            extension.AddExtension(serviceCollection);
        }
        serviceCollection.AddHostedService<ResetMessageBackgroundService>();
        serviceCollection.AddHostedService<ClearOldMessagesBackgroundService>();
        return serviceCollection;
    }
}