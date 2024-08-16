using LightMQ.Consumer;
using LightMQ.Internal;
using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ.BackgroundService;

public class DispatcherService : IHostedService
{
    private readonly ILogger<DispatcherService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IOptions<LightMQOptions> _options;
    private readonly IConsumerProvider _consumerProvider;
    private CancellationTokenSource _cancel;

    protected List<IPollMessageTask> _tasks;

    public DispatcherService(ILogger<DispatcherService> logger,IServiceProvider serviceProvider,IOptions<LightMQOptions> options,IConsumerProvider consumerProvider)
    {
        _logger     = logger;
        _serviceProvider = serviceProvider;
        _options = options;
        _consumerProvider = consumerProvider;
        _tasks = new();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cancel  = new CancellationTokenSource();
        try
        {
            _logger.LogInformation("LightMQ服务启动");

            _consumerProvider.ScanConsumers();

            if (_consumerProvider.GetConsumerInfos().Count == 0)
            {
                _logger.LogInformation("没有扫描到消费者");
                return Task.CompletedTask;
            };
            
            StartDispatch(_cancel.Token);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "LightMQ扫描消费者出现异常");

        }
        return Task.CompletedTask;
    }

    private void StartDispatch(CancellationToken cancellationToken)
    {
        foreach (var consumer in _consumerProvider.GetConsumerInfos())
        {
            for (var i = 0; i < consumer.ConsumerOptions.ParallelNum; i++)
            {
                var scheduleConsumeTask = _serviceProvider.GetRequiredService<IPollMessageTask>();
                _tasks.Add(scheduleConsumeTask);
                Task.Factory.StartNew(
                    async () => await scheduleConsumeTask.RunAsync(consumer, cancellationToken),
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                );
            }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cancel.Cancel();

        try
        {
            var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(_options.Value.ExitTimeOut);
            while(true)
            {
                tokenSource.Token.ThrowIfCancellationRequested();
                
                if (_tasks.All(it => it.IsRunning == false))
                    break;

                var topics=_tasks.Where(it => it.IsRunning).Select(it=>it.GetConsumerInfo()!.ConsumerOptions.Topic);
                _logger.LogWarning($"主题：{string.Join(",",topics)}的消费者还在运行中，等待结束...");
                await Task.Delay(1000,tokenSource.Token);
            }
        }
        catch (Exception)
        {
            _logger.LogInformation("LightMQ等待退出超时，强制退出");
        }

        _logger.LogInformation("LightMQ服务停止");
    }
}