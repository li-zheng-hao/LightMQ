
using Newtonsoft.Json;

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
    /// 更新时间
    /// </summary>
    public DateTime UpdateTime { get; set; }

    /// <summary>
    /// 可执行时间
    /// </summary>
    public DateTime ExecutableTime { get; set; }
    
    /// <summary>
    /// 重试次数
    /// </summary>
    public int RetryCount { get; set; }
    
    /// <summary>
    /// 消息头信息 反序列化为Dictionary[string, string] 类型
    /// </summary>
    public string? Header { get; set; }
    
    /// <summary>
    /// 队列名
    /// </summary>
    public string? Queue{get;set;}
}

/// <summary>
/// 消息扩展
/// </summary>
public static class MessageExtension
{
    /// <summary>
    /// 设置消息头
    /// </summary>
    /// <param name="message"></param>
    /// <param name="header"></param>
    public static void SetHeader(this Message message, Dictionary<string, string> header)
    {
        message.Header = JsonConvert.SerializeObject(header);
    }
    
    /// <summary>
    /// 新增消息头
    /// </summary>
    /// <param name="message"></param>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public static void AddHeader(this Message message, string key,string value)
    {
        var header=message.GetHeader();

        if (header== null)
        {
            header=new Dictionary<string, string>();
        }
        
        header!.Add(key,value);
        
        message.SetHeader(header);
    }
    /// <summary>
    /// 返回消息头
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public static Dictionary<string, string>? GetHeader(this Message message)
    {
        return JsonConvert.DeserializeObject<Dictionary<string, string>>(message.Header??string.Empty);
    }
}