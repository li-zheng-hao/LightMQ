# LightMQ

[English](README.md) | [中文](./README_CN.md)

## Introduction

Database-based message queues, currently supported databases.：

1. MongoDB
2. SqlServer

Features：

1. Supports retries
2. Supports multiple queues
3. Supports concurrent consumption
4. Support opentelemetry tracing

## Test Coverage

![test screenshot](./doc/test_coverage_20240822100230.jpg)

## Usage

Initialize：

```c#

serviceCollection.AddLightMQ(it =>
{
    // it.UseSqlServer("Data Source=.;Initial Catalog=Test;User ID=sa;Password=Abc12345;");
    it.UseMongoDB("mongodb://localhost:27017","Test");
});

```

Add a consumer：

```c#
public class Test2Consumer:IMessageConsumer
{

    public ConsumerOptions GetOptions()
    {
        return new ConsumerOptions()
        {
            ParallelNum = 1,
            Topic = "test"
        };
    }

    public async Task<bool> ConsumeAsync(string message, CancellationToken cancellationToken)
    {
        Console.WriteLine("consume message:"+message);
        await Task.Delay(2_000,cancellationToken);
        return true;
    }

  
}
```

Register the consumer：

```C#
builder.Services.AddScoped<TestConsumer>();
```

## Consumer Options

```c#
public class ConsumerOptions
{
    /// <summary>
    /// Topic
    /// </summary>
    public string Topic { get; set; }
    
    /// <summary>
    /// Enable Random Queue
    /// </summary>
    public bool EnableRandomQueue {get;set;}
    
    /// <summary>
    /// Poll Interval
    /// </summary>
    public TimeSpan PollInterval { get; set; }=TimeSpan.FromSeconds(2);

    /// <summary>
    /// Retry Count (not including the first execution)
    /// </summary>
    public int RetryCount { get; set; } = 0;

    /// <summary>
    /// Retry Interval
    /// </summary>
    public TimeSpan RetryInterval { get; set; }=TimeSpan.FromSeconds(5);
    
    /// <summary>
    /// Concurrent Number
    /// </summary>
    public int ParallelNum { get; set; }
}
```

More examples can be found under the Sample category.

## Develop Tool

![JetBrains](https://resources.jetbrains.com/storage/products/company/brand/logos/Rider_icon.png)

