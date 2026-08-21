using System.Reflection;

namespace LightMQ.Options;

public class LightMQOptions
{
    public LightMQOptions()
    {
        MessageExpireDuration = TimeSpan.FromDays(7);
        Extensions = new();
        ConsumerAssembly = Assembly.GetEntryAssembly();
        ExitTimeOut = TimeSpan.FromSeconds(10);
    }

    public List<IExtension> Extensions { get; set; }

    /// <summary>
    /// 消息过期时间 超过这个时间的消息要删除(所有状态)
    /// </summary>
    public TimeSpan MessageExpireDuration { get; set; }

    /// <summary>
    /// 消息存储表名
    /// </summary>
    public string TableName { get; set; } = "lightmq_messages";

    /// <summary>
    /// 消费者所在dll
    /// </summary>
    public Assembly? ConsumerAssembly { get; set; }

    /// <summary>
    /// 重置超时消息的扫描间隔，默认30秒。
    /// 多节点部署时内部通过分布式租约自动选举主节点，只有主节点执行重置扫描。
    /// </summary>
    public TimeSpan ResetOutOfDateInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 服务退出超时时间 默认10秒
    /// </summary>
    public TimeSpan ExitTimeOut { get; set; }
}
