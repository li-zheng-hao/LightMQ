using LightMQ.Options;

namespace LightMQ.Storage.Sqlite;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 使用Sqlite存储
    /// Sqlite版本必须大于3.35.0 (需要支持RETURNING语句)
    /// </summary>
    /// <param name="mqOptions"></param>
    /// <param name="connectionstring">连接字符串</param>
    /// <returns></returns>
    public static LightMQOptions UseSqlite(this LightMQOptions mqOptions,string connectionstring="Data Source=lightmq.db")
    {
        mqOptions.Extensions.Add(new SqliteExtension(connectionstring));
        return mqOptions;
    }
}