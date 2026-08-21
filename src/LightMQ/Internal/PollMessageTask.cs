using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using LightMQ.Consumer;
using LightMQ.Diagnostics;
using LightMQ.Options;
using LightMQ.Storage;
using LightMQ.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace LightMQ.Internal;

internal class PollMessageTask:IPollMessageTask
{
    protected static readonly DiagnosticListener _diagnosticListener =
        new(DiagnosticsListenserNames.DiagnosticListenerName);

    /// <summary>
    /// 每次批量领取的最大消息条数
    /// </summary>
    private const int PollBatchCount = 20;

    private readonly ILogger<PollMessageTask> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IStorageProvider _storageProvider;
    private ConsumerInfo? _consumerInfo;
    private string? _lastQueue;

    /// <summary>
    /// 已领取但还未完成处理(ACK/NACK/重试/重新入队)的消息，用于任务退出时批量重置状态
    /// </summary>
    private readonly ConcurrentDictionary<string, Message> _leasedMessages = new();

    /// <summary>
    /// 已领取并写入Channel但还未被消费者取走的消息数，用于限制领取数量不超过Channel容量
    /// </summary>
    private int _pendingClaimCount;

    /// <summary>
    /// 连续空轮询次数，用于空队列时指数退避，避免空转轮询数据库
    /// </summary>
    private int _consecutiveEmptyPolls;

    public PollMessageTask(ILogger<PollMessageTask> logger,IServiceProvider serviceProvider,IStorageProvider storageProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
        _storageProvider = storageProvider;
    }

    /// <summary>
    /// 是否正在运行
    /// </summary>
    public bool IsRunning { get;private set; }

    public ConsumerInfo? GetConsumerInfo()
    {
        return _consumerInfo;
    }

    /// <summary>
    /// 运行一个消费者的消费任务：
    /// 单个生产者线程从数据库批量领取消息写入有界 Channel，
    /// ParallelNum 个消费者线程从 Channel 取消息执行消费方法并确认。
    /// 相比"每个并发数各自轮询一次数据库"，数据库轮询频率从 ParallelNum/轮询间隔 降到 1/轮询间隔。
    /// </summary>
    public async Task RunAsync(ConsumerInfo consumerInfo,CancellationToken stoppingToken)
    {
        _consumerInfo = consumerInfo;
        IsRunning = true;

        // ParallelNum 表示本地消费并发度；未配置时默认按 1 处理，避免消费者不消费
        var parallelNum = Math.Max(1, consumerInfo.ConsumerOptions.ParallelNum);
        if (consumerInfo.ConsumerOptions.ParallelNum <= 0)
            _logger.LogWarning($"{consumerInfo.ConsumerOptions.Topic}消费者没有配置ParallelNum，默认使用1个并发");

        // Channel 容量即本地缓冲大小：配合批量领取既能提高吞吐，又不会向数据库无限领取
        var channel = Channel.CreateBounded<Message>(new BoundedChannelOptions(parallelNum)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        });

        try
        {
            var producerTask = Task.Run(
                () => PollAndPublishAsync(channel.Writer, parallelNum, stoppingToken),
                CancellationToken.None
            );
            var allTasks = Enumerable.Range(0, parallelNum)
                .Select(_ => Task.Run(
                    () => ConsumeFromChannelAsync(channel.Reader, stoppingToken),
                    CancellationToken.None
                ))
                .Append(producerTask)
                .ToArray();

            // 取消时任务会尽快退出，这里先接收可能的取消异常
            try
            {
                await Task.WhenAll(allTasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.LogError(e, "消费消息出现未处理异常");
            }

            // 等待所有生产者/消费者彻底退出（例如正在处理中的消息自然完成或响应取消）
            try
            {
                await Task.WhenAll(allTasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (Exception e)
            {
                _logger.LogError(e, "消费消息收尾时出现未处理异常");
            }

            // 将缓冲中未处理的消息重置为等待消费，避免任务退出后消息停留在Processing状态
            await ResetLeasedMessagesAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await ResetLeasedMessagesAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "消费消息出现未处理异常");
        }
        finally
        {
            _logger.LogInformation($"{_consumerInfo!.ConsumerOptions.Topic}主题消费者停止消费");
            IsRunning = false;
        }
    }

    /// <summary>
    /// 生产者：按 Channel 可用空间批量领取消息并写入 Channel，
    /// 队列为空时按 PollInterval 休眠等待。
    /// </summary>
    private async Task PollAndPublishAsync(ChannelWriter<Message> writer, int capacity, CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // 只领取能放得下的消息，避免批量领取过多导致消息长时间停留在Processing状态
                    var freeSlots = capacity - Volatile.Read(ref _pendingClaimCount);
                    if (freeSlots <= 0)
                    {
                        await writer.WaitToWriteAsync(stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    var messages = await PollNewMessagesAsync(Math.Min(PollBatchCount, freeSlots), stoppingToken).ConfigureAwait(false);
                    if (messages.Count == 0)
                    {
                        // 空队列退避：连续领不到时等待时间翻倍，上限为 PollInterval 的 2 倍，
                        // 队列空闲时数据库基本静默，有消息时立即恢复高频轮询
                        _consecutiveEmptyPolls++;
                        await Task.Delay(GetEmptyQueueDelay(), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    // 领取到消息，重置退避
                    _consecutiveEmptyPolls = 0;
                    foreach (var message in messages)
                    {
                        // 先登记已领取消息，再写入，即使中途取消也能在退出时重置状态
                        _leasedMessages.TryAdd(message.Id, message);
                        // 领取数量不会超过可用空间，写入不会阻塞
                        await writer.WriteAsync(message, stoppingToken).ConfigureAwait(false);
                        Interlocked.Increment(ref _pendingClaimCount);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "拉取消息出现异常");
                    await Task.Delay(_consumerInfo!.ConsumerOptions.PollInterval, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    /// <summary>
    /// 计算空队列时的退避等待时间：首次空轮询按 PollInterval，之后翻倍并封顶为 PollInterval 的 2 倍
    /// </summary>
    private TimeSpan GetEmptyQueueDelay()
    {
        var interval = _consumerInfo!.ConsumerOptions.PollInterval;
        // 已在调用前自增，_consecutiveEmptyPolls >= 2 说明连续至少两次领不到
        if (_consecutiveEmptyPolls < 2 || interval.Ticks > TimeSpan.MaxValue.Ticks / 2)
            return interval;
        return TimeSpan.FromTicks(interval.Ticks * 2);
    }

    /// <summary>
    /// 消费者：从 Channel 取消息执行消费
    /// </summary>
    private async Task ConsumeFromChannelAsync(ChannelReader<Message> reader, CancellationToken stoppingToken)
    {
        try
        {
            while (await reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var message))
                {
                    Interlocked.Decrement(ref _pendingClaimCount);
                    await ConsumeMessageAsync(message, stoppingToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task ConsumeMessageAsync(Message message, CancellationToken stoppingToken)
    {
        try
        {
            // 变为消费状态
            message.Status = MessageStatus.Processing;

            TracingBefore(message);

            using var scope = _serviceProvider.CreateScope();

            var consumer =
                scope.ServiceProvider.GetService(_consumerInfo!.ConsumerType) as IMessageConsumer;

            if (message.RetryCount > 0)
                _logger.LogInformation($"第{message.RetryCount + 1}次重试消息{message.Id}");

            var result = await consumer!.ConsumeAsync(message.Data, stoppingToken).ConfigureAwait(false);

            if (result.Requeue)
                await _storageProvider.RequeueMessageAsync(message).ConfigureAwait(false);
            else if (result.IsSuccess)
                await _storageProvider.AckMessageAsync(message).ConfigureAwait(false);
            else
            {
                if (message.RetryCount < _consumerInfo.ConsumerOptions.RetryCount)
                {
                    message.RetryCount += 1;
                    message.ExecutableTime = DateTime.Now.Add(_consumerInfo.ConsumerOptions.RetryInterval);
                    await _storageProvider.UpdateRetryInfoAsync(message).ConfigureAwait(false);
                }
                else
                    await _storageProvider.NackMessageAsync(message).ConfigureAwait(false);
            }

            _leasedMessages.TryRemove(message.Id, out _);
        }
        catch (Exception e)
        {
            if (e is OperationCanceledException)
            {
                // 消费者正在处理消息时被取消，则重置消息状态
                _logger.LogInformation($"当前消息[ID={message.Id},Topic={message.Topic}]重置消息状态为等待消费");
                await _storageProvider.ResetMessageAsync(message).ConfigureAwait(false);
                _logger.LogInformation($"当前消息[ID={message.Id},Topic={message.Topic}]重置消息状态为等待消费成功");

                _leasedMessages.TryRemove(message.Id, out _);
                throw;
            }

            _logger.LogError(e, $"{_consumerInfo!.ConsumerOptions.Topic}消费消息异常");

            if (message.RetryCount < _consumerInfo.ConsumerOptions.RetryCount)
            {
                message.RetryCount += 1;
                message.ExecutableTime = DateTime.Now.Add(_consumerInfo.ConsumerOptions.RetryInterval);
                await _storageProvider.UpdateRetryInfoAsync(message).ConfigureAwait(false);
            }
            else
                await _storageProvider.NackMessageAsync(message).ConfigureAwait(false);

            _leasedMessages.TryRemove(message.Id, out _);
        }
        finally
        {
            TracingAfter(message);
        }
    }

    private async Task<List<Message>> PollNewMessagesAsync(int count, CancellationToken stoppingToken)
    {
        var topic = _consumerInfo!.ConsumerOptions.Topic;
        List<Message> messages;

        // 开启了随机队列，且上一个消息的队列名不是空
        if (_consumerInfo.ConsumerOptions.EnableRandomQueue)
        {
            var allQueues = await _storageProvider.PollAllQueuesAsync(topic, stoppingToken).ConfigureAwait(false);
            if (allQueues.Any() == false)
                messages = await _storageProvider.PollNewMessagesAsync(topic, count, stoppingToken).ConfigureAwait(false);
            else if (allQueues.Count() == 1)
                messages = await _storageProvider.PollNewMessagesAsync(topic, allQueues[0], count, stoppingToken).ConfigureAwait(false);
            else
            {
                var queues = new List<string?>(allQueues);
                if (queues.Contains(_lastQueue))
                    queues.Remove(_lastQueue);

                var queue = GetRandomQueue(queues);
                messages = await _storageProvider.PollNewMessagesAsync(topic, queue, count, stoppingToken).ConfigureAwait(false);
            }

            if (messages.Count > 0)
                _lastQueue = messages[^1].Queue;
        }
        // 没有开启随机队列
        else
            messages = await _storageProvider.PollNewMessagesAsync(topic, count, stoppingToken).ConfigureAwait(false);

        return messages;
    }

    /// <summary>
    /// 任务退出时，将已领取但未完成处理的消息重置为等待消费
    /// </summary>
    private async Task ResetLeasedMessagesAsync()
    {
        if (_leasedMessages.IsEmpty)
            return;

        foreach (var message in _leasedMessages.Values)
        {
            try
            {
                _logger.LogInformation($"当前消息[ID={message.Id},Topic={message.Topic}]重置消息状态为等待消费");
                await _storageProvider.ResetMessageAsync(message).ConfigureAwait(false);
                _logger.LogInformation($"当前消息[ID={message.Id},Topic={message.Topic}]重置消息状态为等待消费成功");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "重置消息状态出现异常");
            }
            finally
            {
                _leasedMessages.TryRemove(message.Id, out _);
            }
        }
    }

    private static string? GetRandomQueue(List<string?> allQueues)
    {
        int index = Random.Shared.Next(allQueues.Count);
        return allQueues[index];
    }
    #region Tracing

    private static void TracingBefore(Message message)
    {
        if (_diagnosticListener.IsEnabled(DiagnosticsListenserNames.BeforeConsume))
        {
            _diagnosticListener.Write(DiagnosticsListenserNames.BeforeConsume, message);
        }
    }

    private static void TracingAfter(Message message)
    {
        if (_diagnosticListener.IsEnabled(DiagnosticsListenserNames.AfterConsume))
        {
            _diagnosticListener.Write(DiagnosticsListenserNames.AfterConsume, message);
        }
    }

    #endregion
}