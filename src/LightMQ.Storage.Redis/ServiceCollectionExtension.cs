using LightMQ.Options;

namespace LightMQ.Storage.Redis;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 使用Redis存储
    /// </summary>
    /// <param name="mqOptions"></param>
    /// <param name="connectionString">Redis连接字符串 如 localhost:6379</param>
    /// <returns></returns>
    public static LightMQOptions UseRedis(this LightMQOptions mqOptions, string connectionString = "localhost:6379")
    {
        mqOptions.Extensions.Add(new RedisExtension(connectionString));
        return mqOptions;
    }
}