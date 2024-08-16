using LightMQ.BackgroundService;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Publisher;
using Microsoft.Extensions.DependencyInjection;

namespace LightMQ;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="serviceCollection"></param>
    /// <returns></returns>
    public static IServiceCollection AddLightMQ(this IServiceCollection serviceCollection,Action<LightMQOptions> configure)
    {
        serviceCollection.Configure(configure);
        
        var options = new LightMQOptions();
        configure(options);
        
        foreach (var extension in options.Extensions)
        {
            extension.AddExtension(serviceCollection);
        }

        serviceCollection.AddSingleton<IMessagePublisher, MessagePublisher>();
        serviceCollection.AddSingleton<IConsumerProvider, ConsumerProvider>();
        serviceCollection.AddTransient<IPollMessageTask,PollMessageTask>();
        serviceCollection.AddHostedService<InitStorageBackgroundService>();
        serviceCollection.AddHostedService<ResetMessageBackgroundService>();
        serviceCollection.AddHostedService<ClearOldMessagesBackgroundService>();
        serviceCollection.AddHostedService<DispatcherService>();
        return serviceCollection;
    }
}