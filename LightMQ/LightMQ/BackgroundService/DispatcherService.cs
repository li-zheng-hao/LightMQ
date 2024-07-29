using System.Diagnostics;
using LightMQ.Consumer;
using LightMQ.Diagnostics;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.BackgroundService;

public class DispatcherService : IHostedService
{
    private readonly ILogger<DispatcherService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IStorageProvider _storageProvider;
    private readonly IOptions<LightMQOptions> _options;
    private Dictionary<string,Type> _consumers;
    private Dictionary<string,ConsumerOptions> _consumersOptions;
    private CancellationTokenSource _cancel;

    private List<PollMessageTask> _tasks;

    public DispatcherService(ILogger<DispatcherService> logger,IServiceProvider serviceProvider,IStorageProvider storageProvider,IOptions<LightMQOptions> options)
    {
        _logger     = logger;
        _serviceProvider = serviceProvider;
        _storageProvider = storageProvider;
        _options = options;
        _consumers = new();
        _consumersOptions = new();
        _tasks = new();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancel  = new CancellationTokenSource();
        try
        {
            _logger.LogInformation("LightMQ Dispatcher Service Started at {Now}", DateTime.Now);

            InitConsumers();

            if (_consumers.Count == 0)
            {
                _logger.LogInformation("没有扫描到消费者");
                return Task.CompletedTask;
            };
            
            StartDispatch(_cancel.Token);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "LightMQ Dispatcher Service Start Failed");

        }
        return Task.CompletedTask;
    }

    private void StartDispatch(CancellationToken cancellationToken)
    {
        foreach (var consumer in _consumersOptions)
        {
            for (var i = 0; i < consumer.Value.ParallelNum; i++)
            {
                var scheduleConsumeTask = _serviceProvider.GetRequiredService<PollMessageTask>();
                _tasks.Add(scheduleConsumeTask);
                Task.Factory.StartNew(
                    async () => await scheduleConsumeTask.RunAsync(consumer.Value,_consumers[consumer.Key], cancellationToken),
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                );
            }
        }
    }

    private void InitConsumers()
    {
        var consumersTypes=_options.Value.ConsumerAssembly.ExportedTypes.Where(it => typeof(IMessageConsumer).IsAssignableFrom(it))
            .ToList();

        using var scope = _serviceProvider.CreateScope();
        foreach (var consumersType in consumersTypes)
        {
            var consumer=scope.ServiceProvider.GetService(consumersType) as IMessageConsumer;
            if (consumer is null)
            {
                _logger.LogWarning($"扫描到了{consumersType.FullName}，但是没有从IOC容器中获取到实例，跳过");
                continue;
            }
            _consumers.Add(consumer.GetOptions().Topic,consumersType);
            _consumersOptions.Add(consumer.GetOptions().Topic,consumer.GetOptions());
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cancel.Cancel();

        try
        {
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(_options.Value.ExitTimeOut);
            while (!tokenSource.IsCancellationRequested)
            {
                if (_tasks.All(it => it.IsRunning == false))
                    break;

                var topics=_tasks.Where(it => it.IsRunning).Select(it=>it.ConsumerOptions?.Topic);
                _logger.LogWarning($"主题：{string.Join(",",topics)}的消费者还在运行中，等待结束...");
                await Task.Delay(1000,tokenSource.Token);
            }
        }
        catch (Exception)
        {
            _logger.LogInformation("LightMQ等待退出超时，强制退出");
        }

        _logger.LogInformation("LightMQ Dispatcher Service Stopped at {Now}", DateTime.Now);
    }
 
}