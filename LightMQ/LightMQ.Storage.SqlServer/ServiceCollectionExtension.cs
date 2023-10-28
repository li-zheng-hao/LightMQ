using LightMQ.Options;

namespace LightMQ.Storage.SqlServer;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 使用SqlServer存储
    /// </summary>
    /// <param name="serviceCollection"></param>
    /// <param name="connectionstring">连接字符串</param>
    /// <param name="databaseName">数据库名称</param>
    /// <returns></returns>
    public static LightMQOptions UseSqlServer(this LightMQOptions mqOptions,string connectionstring)
    {
        mqOptions.Extensions.Add(new SqlServerExtension(connectionstring));
        
        return mqOptions;
    }
}