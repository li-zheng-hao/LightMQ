using LightMQ.Storage;
using LightMQ.Storage.MongoDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LightMQ.UnitTest;

public class MongoDbExtensionTests
{
    [Fact]
    public void UseMongo_ShouldAddMongoExtensions()
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddLightMQ(it =>
        {
            it.UseMongoDB("connectionstring", "database");
        });
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var storageProvider = serviceProvider.GetRequiredService<IStorageProvider>();
        Assert.IsType<MongoStorageProvider>(storageProvider);
        // Assert connection string is correct
        var options = serviceProvider.GetRequiredService<IOptions<MongoDBOptions>>();
        Assert.Equal("connectionstring", options.Value.ConnectionString);
        Assert.Equal("database", options.Value.DatabaseName);
    }
}