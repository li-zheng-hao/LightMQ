
namespace LightMQ.Transport;

public class Message
{
    /// <summary>
    /// 消息唯一id
    /// </summary>
    public string Id { get; set; }
    
    /// <summary>
    /// 状态
    /// </summary>
    public MessageStatus Status { get; set; }
    
    /// <summary>
    /// json数据
    /// </summary>
    public string Data { get; set; }
    
    /// <summary>
    /// 主题
    /// </summary>
    public string Topic { get; set; }
    
    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreateTime { get; set; }
    
    /// <summary>
    /// 可执行时间
    /// </summary>
    public DateTime ExecutableTime { get; set; }
    
    /// <summary>
    /// 重试次数
    /// </summary>
    public int RetryCount { get; set; }
}