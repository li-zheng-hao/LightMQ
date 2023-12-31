﻿using System.Reflection;

namespace LightMQ.Options;

public class LightMQOptions
{
    public LightMQOptions()
    {
        MessageTimeoutDuration=TimeSpan.FromMinutes(5);
        MessageExpireDuration=TimeSpan.FromDays(7);
        Extensions = new();
        ConsumerAssembly = Assembly.GetEntryAssembly();
    }
    
    public List<IExtension> Extensions { get; set; }
    /// <summary>
    /// 消息超时时间 超过这个时间的消息状态要重置为待处理
    /// </summary>
    public TimeSpan MessageTimeoutDuration { get; set; }
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
    public Assembly ConsumerAssembly { get; set; }
    
}