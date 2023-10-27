namespace SW.Core.MongoMQ.Consumer;

public class MongoConsumerOptions
{
    /// <summary>
    /// 主题
    /// </summary>
    public string Topic { get; set; }
    
    /// <summary>
    /// 拉取间隔
    /// </summary>
    public TimeSpan PollInterval { get; set; }=TimeSpan.FromSeconds(2);
}