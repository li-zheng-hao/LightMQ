﻿using System.Diagnostics;
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
    
    private readonly ILogger<PollMessageTask> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly IStorageProvider _storageProvider;
    private ConsumerInfo? _consumerInfo;
    private string? lastQueue;

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
    
    public async Task RunAsync(ConsumerInfo consumerInfo,CancellationToken stoppingToken)
    {
        Message? currentMessage = null;
        
        _consumerInfo = consumerInfo;
        IsRunning = true;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    currentMessage=await PollNewMessageAsync(stoppingToken);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "拉取消息出现异常");
                    currentMessage = null;
                }

                if (currentMessage == null)
                {
                    await Task.Delay(_consumerInfo.ConsumerOptions.PollInterval, stoppingToken);
                    continue;
                }

                try
                {
                    // 变为消费状态
                    currentMessage.Status = MessageStatus.Processing;
                    
                    TracingBefore(currentMessage);

                    using var scope = _serviceProvider.CreateScope();

                    var consumer =
                        scope.ServiceProvider.GetService(consumerInfo.ConsumerType) as IMessageConsumer;

                    if (currentMessage.RetryCount > 0)
                        _logger.LogInformation($"第{currentMessage.RetryCount + 1}次重试消息{currentMessage.Id}");

                    var result = await consumer!.ConsumeAsync(currentMessage.Data, stoppingToken);

                    if (result)
                    {
                        await _storageProvider.AckMessageAsync(currentMessage);
                    }
                    else
                    {
                        if (currentMessage.RetryCount < _consumerInfo.ConsumerOptions.RetryCount)
                        {
                            currentMessage.RetryCount += 1;
                            currentMessage.ExecutableTime = DateTime.Now.Add(_consumerInfo.ConsumerOptions.RetryInterval);
                            await _storageProvider.UpdateRetryInfoAsync(currentMessage);
                        }
                        else
                            await _storageProvider.NackMessageAsync(currentMessage);
                    }
                }
                catch (Exception e)
                {
                    if (e is OperationCanceledException) throw;

                    _logger.LogError(e, $"{_consumerInfo.ConsumerOptions.Topic}消费消息异常");

                    if (currentMessage.RetryCount < _consumerInfo.ConsumerOptions.RetryCount)
                    {
                        currentMessage.RetryCount += 1;
                        currentMessage.ExecutableTime = DateTime.Now.Add(_consumerInfo.ConsumerOptions.RetryInterval);
                        await _storageProvider.UpdateRetryInfoAsync(currentMessage);
                    }
                    else
                        await _storageProvider.NackMessageAsync(currentMessage);
                }
                finally
                {
                    TracingAfter(currentMessage);
                }

            }
        }
        catch (OperationCanceledException)
        {
            if (currentMessage?.Status == MessageStatus.Processing)
            {
                try
                {
                    // 如果消费者正在处理消息，则重置消息状态
                    _logger.LogInformation($"当前消息[ID={currentMessage.Id},Topic={currentMessage.Topic}]重置消息状态为等待消费");
                    await _storageProvider.ResetMessageAsync(currentMessage);
                    _logger.LogInformation($"当前消息[ID={currentMessage.Id},Topic={currentMessage.Topic}]重置消息状态为等待消费成功");

                }
                catch (Exception e)
                {
                    _logger.LogError(e,"重置消息状态出现异常");
                }
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e,$"出现未处理异常");
        }
        
        _logger.LogInformation($"{_consumerInfo.ConsumerOptions.Topic}主题消费者停止消费");
        IsRunning = false;
    }

    private async Task<Message?> PollNewMessageAsync(CancellationToken stoppingToken)
    {
        Message? message;
        // 开启了随机队列，且上一个消息的队列名不是空
        if (_consumerInfo!.ConsumerOptions.EnableRandomQueue)
        {
            var allQueues= await _storageProvider.PollAllQueuesAsync(_consumerInfo!.ConsumerOptions.Topic, stoppingToken);
            if(allQueues.Any()==false)
                message = await _storageProvider.PollNewMessageAsync(_consumerInfo!.ConsumerOptions.Topic, stoppingToken);
            else if (allQueues.Count() == 1)
            {
                message = await _storageProvider.PollNewMessageAsync(_consumerInfo!.ConsumerOptions.Topic,allQueues[0], stoppingToken);
            }
            else
            {
                if (allQueues.Contains(lastQueue))
                {
                    allQueues.Remove(lastQueue);
                }

                var queue = GetRandomQueue(allQueues);
                message = await _storageProvider.PollNewMessageAsync(_consumerInfo!.ConsumerOptions.Topic,queue, stoppingToken);
            }
            
            if(message != null) lastQueue = message.Queue;
        }
        // 没有开启随机队列
        else
        {
            message = await _storageProvider.PollNewMessageAsync(_consumerInfo!.ConsumerOptions.Topic, stoppingToken);
        }
        return message;
    }
    private  string? GetRandomQueue(List<string?> allQueues)
    {
        Random random = new Random();
        int index = random.Next(allQueues.Count); // 生成一个随机索引
        return allQueues[index]; // 返回对应索引的字符串
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