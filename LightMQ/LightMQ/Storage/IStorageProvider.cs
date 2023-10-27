using LightMQ.Transport;

namespace LightMQ.Storage;

public interface IStorageProvider
{
    /// <summary>
    ///  清除旧数据
    /// </summary>
    /// <returns></returns>
    Task ClearOldMessagesAsync(CancellationToken cancellationToken);
    
    /// <summary>
    ///  重置消息状态
    /// </summary>
    /// <returns></returns>
    Task ResetProcessingMessagesAsync(IMessage message,CancellationToken cancellationToken);
    
    /// <summary>
    ///  重置超时消息
    /// </summary>
    /// <returns></returns>
    Task ResetOutOfDateMessagesAsync(CancellationToken cancellationToken);

    /// <summary>
    /// 拉取新消息
    /// </summary>
    /// <param name="stoppingToken"></param>
    /// <returns></returns>
    Task<IMessage> PollNewMessageAsync(CancellationToken cancellationToken);
}