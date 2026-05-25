namespace LightMQ.Storage.Redis;

public class RedisOptions
{
    public string ConnectionString { get; set; } = "localhost:6379";

    public string KeyPrefix { get; set; } = "lightmq";
}