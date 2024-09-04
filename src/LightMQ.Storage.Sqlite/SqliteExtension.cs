using LightMQ.Options;
using Microsoft.Extensions.DependencyInjection;

namespace LightMQ.Storage.Sqlite;

public class SqliteExtension:IExtension
{
    private readonly string _connectionstring;

    public SqliteExtension(string connectionstring)
    {
        _connectionstring = connectionstring;
    }

    public IServiceCollection AddExtension(IServiceCollection serviceCollection)
    {
        serviceCollection.Configure<SqliteOptions>(it =>
        {
            it.ConnectionString = _connectionstring;
        });
        serviceCollection.AddSingleton<IStorageProvider, SqliteStorageProvider>();
        return serviceCollection;
    }
}