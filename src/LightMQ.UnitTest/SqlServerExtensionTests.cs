using LightMQ.Storage;
using LightMQ.Storage.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LightMQ.UnitTest;

public class SqlServerExtensionTests
{
    [Fact]
    public void AddExtension_ShouldAddSqlServerExtension()
    {
        var serviceCollection = new ServiceCollection();
        var extension = new SqlServerExtension("connectionstring");
        extension.AddExtension(serviceCollection);
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var storageProvider = serviceProvider.GetRequiredService<IStorageProvider>();
        Assert.IsType<SqlServerStorageProvider>(storageProvider);
        // Assert connection string is correct
        var options = serviceProvider.GetRequiredService<IOptions<SqlServerOptions>>();
        Assert.Equal("connectionstring", options.Value.ConnectionString);
    }
    [Fact]
    public void UseSqlserver_ShouldAddSqlServerExtension()
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddLightMQ(it =>
        {
            it.UseSqlServer("connectionstring");
        });
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var storageProvider = serviceProvider.GetRequiredService<IStorageProvider>();
        Assert.IsType<SqlServerStorageProvider>(storageProvider);
        // Assert connection string is correct
        var options = serviceProvider.GetRequiredService<IOptions<SqlServerOptions>>();
        Assert.Equal("connectionstring", options.Value.ConnectionString);
    }
}