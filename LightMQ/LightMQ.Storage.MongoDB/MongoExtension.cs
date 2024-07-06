using LightMQ.Options;
using LightMQ.Publisher;
using LightMQ.Storage;
using LightMQ.Storage.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace SW.Core.MongoMQ;

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