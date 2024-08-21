namespace LightMQ.Internal;

public interface IPollMessageTask
{
    /// <summary>
    /// 是否正在运行
    /// </summary>
    public bool IsRunning { get; }

    /// <summary>
    /// 获取当前任务的消费者信息
    /// </summary>
    /// <returns></returns>
    ConsumerInfo? GetConsumerInfo();

    /// <summary>
    /// 运行
    /// </summary>
    /// <param name="consumerInfo"></param>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    Task RunAsync(ConsumerInfo consumerInfo, CancellationToken stoppingToken);
}