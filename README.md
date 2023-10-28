# LightMQ

## 介绍

基于数据库的消息队列，目前支持的数据库：

1. MongoDB
2. SqlServer

## 使用方式

初始化：

```c#

serviceCollection.AddLightMQ(it =>
{
    // it.UseSqlServer("Data Source=.;Initial Catalog=Test;User ID=sa;Password=Abc12345;");
    it.UseMongoDB("mongodb://localhost:27017","Test");
});

```

新增消费者：

```c#
public class TestConsumer:MessageConsumerBase
{
    public TestConsumer(IStorageProvider storageProvider) : base(storageProvider)
    {
    }

    public override ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            Topic = "test",
            PollInterval = TimeSpan.FromSeconds(2)
        };
    }

    public override Task ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine(message);
        return Task.CompletedTask;
    }
}
```

注册消费者：

```C#
serviceCollection.AddHostedService<TestConsumer>();
```