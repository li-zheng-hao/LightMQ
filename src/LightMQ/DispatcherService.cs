using LightMQ.BackgroundService;
using LightMQ.Internal;
using LightMQ.Options;
using LightMQ.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LightMQ;

public class DispatcherService : IHostedService
{
    private readonly ILogger<DispatcherService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IStorageProvider _storageProvider;
    private readonly IOptions<LightMQOptions> _options;
    private readonly IConsumerProvider _consumerProvider;
    private CancellationTokenSource _cancel;

    protected List<IPollMessageTask> _tasks;
    private IEnumerable<IBackgroundService> _backgroundServices;

    public DispatcherService(
        ILogger<DispatcherService> logger,
        IServiceProvider serviceProvider,
        IStorageProvider storageProvider,
        IOptions<LightMQOptions> options,
        IConsumerProvider consumerProvider,
        IEnumerable<IBackgroundService> backgroundServices
    )
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _storageProvider = storageProvider;
        _options = options;
        _consumerProvider = consumerProvider;
        _tasks = new();
        _backgroundServices = backgroundServices;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Run(cancellationToken);
    }

    private async Task Run(CancellationToken cancellationToken)
    {
        _cancel = new CancellationTokenSource();
        try
        {
            _logger.LogInformation("LightMQ服务启动");

            await _storageProvider.InitTables(cancellationToken);

            _consumerProvider.ScanConsumers();

            if (_consumerProvider.GetConsumerInfos().Count == 0)
            {
                _logger.LogInformation("没有扫描到消费者");
            }

            StartPollMessageTasks(_cancel.Token);

            StartBackgroundServices(_cancel.Token);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "LightMQ扫描消费者出现异常");
        }
    }

    private void StartBackgroundServices(CancellationToken cancelToken)
    {
        foreach (var task in _backgroundServices)
        {
            Task.Factory.StartNew(
                async () => await task.ExecuteAsync(cancelToken),
                cancelToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
            );
        }
    }

    private void StartPollMessageTasks(CancellationToken cancellationToken)
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
            using var tokenSource = new CancellationTokenSource();
            tokenSource.CancelAfter(_options.Value.ExitTimeOut);
            while (true)
            {
                tokenSource.Token.ThrowIfCancellationRequested();

                if (_tasks.All(it => it.IsRunning == false))
                    break;

                var topics = _tasks
                    .Where(it => it.IsRunning)
                    .Select(it => it.GetConsumerInfo()!.ConsumerOptions.Topic);
                _logger.LogWarning(
                    $"主题：{string.Join(",", topics)}的消费者还在运行中，等待结束..."
                );
                await Task.Delay(1000, tokenSource.Token);
            }
        }
        catch (Exception)
        {
            _logger.LogInformation("LightMQ等待退出超时，强制退出");
        }
        finally
        {
            _cancel.Dispose();
        }

        _logger.LogInformation("LightMQ服务停止");
    }
}
