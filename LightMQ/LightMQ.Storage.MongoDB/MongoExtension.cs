using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;

namespace LightMQ.Storage.MongoDB;

public class MongoExtension:IExtension
{
    private readonly string _connectionstring;
    private readonly string _databaseName;

    public MongoExtension(string connectionstring, string databaseName)
    {
        _connectionstring = connectionstring;
        _databaseName = databaseName;
    }

    public IServiceCollection AddExtension(IServiceCollection serviceCollection)
    {
        serviceCollection.Configure<MongoDBOptions>(it =>
        {
            it.ConnectionString = _connectionstring;
            it.DatabaseName = _databaseName;
        });
        serviceCollection.AddSingleton<IStorageProvider, MongoStorageProvider>();
        return serviceCollection;
    }
}