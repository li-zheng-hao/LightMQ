using LightMQ.Transport;

namespace LightMQ.Storage;

public interface IStorageProvider
{
    /// <summary>
    /// 新增消息
    /// </summary>
    /// <param name="message"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task PublishNewMessageAsync(Message message,
        CancellationToken cancellationToken = default);
    /// <summary>
    /// 新增消息
    /// </summary>
    /// <param name="message"></param>
    /// <param name="transaction">使用事务
    /// MongoDB：IClientSessionHandle
    /// SqlServer：SqlConnection
    /// </param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task PublishNewMessageAsync(Message message, object transaction,
        CancellationToken cancellationToken = default);
    /// <summary>
    ///  清除旧数据
    /// </summary>
    /// <returns></returns>
    Task ClearOldMessagesAsync(CancellationToken cancellationToken=default);
    
    /// <summary>
    ///  消息失败NACK
    /// </summary>
    /// <returns></returns>
    Task NackMessageAsync(Message message,CancellationToken cancellationToken=default);
    /// <summary>
    ///  消息重置
    /// </summary>
    /// <returns></returns>
    Task ResetMessageAsync(Message message,CancellationToken cancellationToken=default);
    /// <summary>
    ///  更新消息的重试信息
    /// </summary>
    /// <returns></returns>
    Task UpdateRetryInfoAsync(Message message,CancellationToken cancellationToken=default);
    /// <summary>
    ///  重置超时消息
    /// </summary>
    /// <returns></returns>
    Task ResetOutOfDateMessagesAsync(CancellationToken cancellationToken=default);

    /// <summary>
    /// 拉取新消息
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task<Message?> PollNewMessageAsync(string topic,CancellationToken cancellationToken=default);
    /// <summary>
    /// 拉取不在指定队列名中的新消息
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="queue"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task<Message?> PollNewMessageAsync(string topic,string? queue,CancellationToken cancellationToken=default);
    

    /// <summary>
    /// 查找待消费消息的queue
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task<List<string?>> PollAllQueuesAsync(string topic,CancellationToken cancellationToken=default);
    /// <summary>
    /// 消息成功ACK
    /// </summary>
    /// <param name="currentMessage"></param>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    Task AckMessageAsync(Message currentMessage, CancellationToken stoppingToken=default);
    /// <summary>
    /// 初始化表
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    Task InitTables(CancellationToken stoppingToken=default);

    Task PublishNewMessagesAsync(List<Message> messages);
    Task PublishNewMessagesAsync(List<Message> messages, object transaction);
}