using LightMQ.Options;

namespace LightMQ.Storage.MongoDB;

public static class ServiceCollectionExtension
{
    /// <summary>
    /// 使用MongoDB存储
    /// </summary>
    /// <param name="serviceCollection"></param>
    /// <param name="connectionstring">连接字符串</param>
    /// <param name="databaseName">数据库名称</param>
    /// <returns></returns>
    public static LightMQOptions UseMongoDB(this LightMQOptions mqOptions,string connectionstring,string databaseName)
    {
        mqOptions.Extensions.Add(new MongoExtension(connectionstring,databaseName));
        
        return mqOptions;
    }
}