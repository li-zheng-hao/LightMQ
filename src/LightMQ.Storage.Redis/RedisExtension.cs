using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;

namespace LightMQ.Storage.Redis;

public class RedisExtension : IExtension
{
    private readonly string _connectionString;

    public RedisExtension(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IServiceCollection AddExtension(IServiceCollection serviceCollection)
    {
        serviceCollection.Configure<RedisOptions>(it =>
        {
            it.ConnectionString = _connectionString;
        });
        serviceCollection.AddSingleton<IStorageProvider, RedisStorageProvider>();
        return serviceCollection;
    }
}