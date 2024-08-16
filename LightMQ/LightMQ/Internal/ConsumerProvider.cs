using System.Reflection;
using LightMQ.Consumer;
using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.Internal;

/// <summary>
/// 
/// </summary>
public class ConsumerProvider:IConsumerProvider
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ConsumerProvider> _logger;
    private readonly IOptions<LightMQOptions> _options;

    private List<ConsumerInfo>? _consumerInfos;

    public ConsumerProvider(IServiceProvider serviceProvider,ILogger<ConsumerProvider> logger,IOptions<LightMQOptions> options)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _options = options;
        
    }

    public void ScanConsumers()
    {
        
        if(_options.Value.ConsumerAssembly==null) throw new ArgumentNullException(nameof(_options.Value.ConsumerAssembly));
        
        var consumersTypes=_options.Value.ConsumerAssembly!.ExportedTypes.Where(it => typeof(IMessageConsumer).IsAssignableFrom(it))
            .ToList();
        
        _consumerInfos = new List<ConsumerInfo>(consumersTypes.Count);
        
        using var scope = _serviceProvider.CreateScope();
        foreach (var consumersType in consumersTypes)
        {
            var consumer=scope.ServiceProvider.GetService(consumersType) as IMessageConsumer;
            if (consumer is null)
            {
                _logger.LogWarning($"扫描到了{consumersType.FullName}，但是没有从IOC容器中获取到实例，跳过");
                continue;
            }
            _consumerInfos.Add(new ConsumerInfo()
            {
                ConsumerType = consumersType,
                ConsumerOptions = consumer.GetOptions()
            });
        }
    }
    
    public List<ConsumerInfo> GetConsumerInfos()
    {
        return _consumerInfos??new List<ConsumerInfo>();
    }
}