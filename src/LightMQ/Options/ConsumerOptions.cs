namespace LightMQ.Options;

public class ConsumerOptions
{
    /// <summary>
    /// 主题
    /// </summary>
    public string Topic { get; set; }

    /// <summary>
    /// 开启随机队列消费
    /// </summary>
    public bool EnableRandomQueue { get; set; }

    /// <summary>
    /// 拉取间隔
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// 重试次数(不包括第一次执行)
    /// </summary>
    public int RetryCount { get; set; } = 0;

    /// <summary>
    /// 重试间隔
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// 并发数量
    /// </summary>
    public int ParallelNum { get; set; }

    /// <summary>
    /// 消费超时重置时间间隔（从开始消费时间开始算） null则不重置
    /// </summary>
    public TimeSpan? ResetInterval { get; set; }
}
