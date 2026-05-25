using LightMQ.Storage;
using LightMQ.Storage.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace LightMQ.UnitTest;

public class RedisExtensionTests
{
    [Fact]
    public void AddExtension_ShouldAddRedisExtension()
    {
        var serviceCollection = new ServiceCollection();
        var extension = new RedisExtension("localhost:6379");
        extension.AddExtension(serviceCollection);
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var storageProvider = serviceProvider.GetRequiredService<IStorageProvider>();
        Assert.IsType<RedisStorageProvider>(storageProvider);
        var options = serviceProvider.GetRequiredService<IOptions<RedisOptions>>();
        Assert.Equal("localhost:6379", options.Value.ConnectionString);
    }

    [Fact]
    public void UseRedis_ShouldAddRedisExtension()
    {
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddLightMQ(it =>
        {
            it.UseRedis("localhost:6380");
        });
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var storageProvider = serviceProvider.GetRequiredService<IStorageProvider>();
        Assert.IsType<RedisStorageProvider>(storageProvider);
        var options = serviceProvider.GetRequiredService<IOptions<RedisOptions>>();
        Assert.Equal("localhost:6380", options.Value.ConnectionString);
    }
}