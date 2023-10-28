using LightMQ.Options;
using LightMQ.Publisher;
using Microsoft.Extensions.DependencyInjection;

namespace LightMQ.Storage.SqlServer;

public class SqlServerExtension:IExtension
{
    private readonly string _connectionstring;

    public SqlServerExtension(string connectionstring)
    {
        _connectionstring = connectionstring;
    }

    public IServiceCollection AddExtension(IServiceCollection serviceCollection)
    {
        serviceCollection.Configure<SqlServerOptions>(it =>
        {
            it.ConnectionString = _connectionstring;
        });
        serviceCollection.AddSingleton<IStorageProvider, SqlServerStorageProvider>();
        return serviceCollection;
    }
}