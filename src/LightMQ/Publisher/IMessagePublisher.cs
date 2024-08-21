namespace LightMQ.Publisher;

public interface IMessagePublisher
{
    /// <summary>
    /// 发送消息
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="message"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(string topic,T message) where T : class;
    /// <summary>
    /// 发送消息
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="message"></param>
    /// <param name="queue">队列名</param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(string topic,T message,string queue) where T : class;
    /// <summary>
    /// 使用事务进行发送，需要在外层自行Commit/Rollback
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="message"></param>
    /// <param name="transaction">使用事务
    /// MongoDB：ISessionHandle
    /// SqlServer：SqlTransaction
    /// </param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(string topic,T message,object transaction) where T : class;

    /// <summary>
    /// 使用事务进行发送，需要在外层自行Commit/Rollback
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="message"></param>
    /// <param name="transaction">使用事务
    /// MongoDB：ISessionHandle
    /// SqlServer：SqlTransaction
    /// </param>
    /// <param name="queue">队列名</param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(string topic,T message,object transaction,string queue) where T : class;
    /// <summary>
    /// 批量发送消息
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="message"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(string topic,List<T> message) where T : class;
    
    /// <summary>
    /// 使用事务进行发送，需要在外层自行Commit/Rollback
    /// </summary>
    /// <param name="topic"></param>
    /// <param name="message"></param>
    /// <param name="transaction">使用事务
    /// MongoDB：ISessionHandle
    /// SqlServer：SqlTransaction
    /// </param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    Task PublishAsync<T>(string topic,List<T> message,object transaction) where T : class;
}