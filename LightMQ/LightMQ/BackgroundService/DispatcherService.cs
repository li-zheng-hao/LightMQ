using LightMQ.Consumer;
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

    public DispatcherService(ILogger<DispatcherService> logger,IServiceProvider serviceProvider,IStorageProvider storageProvider,IOptions<LightMQOptions> options)
    {
        _logger     = logger;
        _serviceProvider = serviceProvider;
        _storageProvider = storageProvider;
        _options = options;
        _consumers = new();
        _consumersOptions = new();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _cancel  = new CancellationTokenSource();
        try
        {
            _logger.LogInformation("LightMQ Dispatcher Service Started at {Now}", DateTime.Now);

            InitConsumers();

            if (_consumers.Count == 0)
            {
                _logger.LogInformation("没有扫描到消费者");
                return;
            };
            
            StartDispatch(_cancel.Token);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "LightMQ Dispatcher Service Start Failed");

        }
    }

    private async Task StartDispatch(CancellationToken cancellationToken)
    {
        foreach (var consumer in _consumersOptions)
        {
            for (var i = 0; i < consumer.Value.ParallelNum; i++)
            {
                Task.Factory.StartNew(
                    async () => await PollingMessage(consumer.Value, cancellationToken),
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                );
            }
        }
    }

    private async Task PollingMessage(ConsumerOptions consumerOptions, CancellationToken stoppingToken)
    {
        Message? currentMessage = null;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                currentMessage = await _storageProvider.PollNewMessageAsync(consumerOptions.Topic, stoppingToken);
            
                if (currentMessage == null)
                {
                    await Task.Delay(consumerOptions.PollInterval, stoppingToken);
                    continue;                
                }

                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var consumer=scope.ServiceProvider.GetService(_consumers[consumerOptions.Topic]) as IMessageConsumer;
                    if(currentMessage.RetryCount>0) _logger.LogInformation($"第{currentMessage.RetryCount+1}次重试消息{currentMessage.Id}");
                    
                    var result=await consumer.ConsumeAsync(currentMessage.Data, stoppingToken);
                    
                    if (result)
                    {
                        await _storageProvider.AckMessageAsync(currentMessage, stoppingToken);
                    }
                    else
                    {
                        if (currentMessage.RetryCount < consumerOptions.RetryCount)
                        {
                            currentMessage.RetryCount += 1;
                            currentMessage.ExecutableTime = DateTime.Now.Add(consumerOptions.RetryInterval);
                            await _storageProvider.UpdateRetryInfoAsync(currentMessage, stoppingToken);
                        }
                        else
                            await _storageProvider.NackMessageAsync(currentMessage, stoppingToken);
                    }
                }
                catch (Exception e)
                {
                    if (e is TaskCanceledException) throw;
                    
                    _logger.LogError(e,$"{consumerOptions.Topic}消费消息异常");
                    
                    if (currentMessage.RetryCount < consumerOptions.RetryCount)
                    {
                        currentMessage.RetryCount += 1;
                        currentMessage.ExecutableTime = DateTime.Now.Add(consumerOptions.RetryInterval);
                        await _storageProvider.UpdateRetryInfoAsync(currentMessage, stoppingToken);
                    }
                    else
                        await _storageProvider.NackMessageAsync(currentMessage, stoppingToken);
                }

            }
        }
        catch (TaskCanceledException)
        {
            _logger.LogInformation($"取消正在执行的任务 {consumerOptions.Topic}");
            
            if (currentMessage?.Status == MessageStatus.Processing)
            {
                await _storageProvider.ResetMessageAsync(currentMessage);
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
        _logger.LogInformation("LightMQ Dispatcher Service Stopped at {Now}", DateTime.Now);
    }
}