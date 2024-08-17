using System.Data.SqlClient;
using LightMQ.Options;
using LightMQ.Storage.MongoDB;
using LightMQ.Storage.SqlServer;
using LightMQ.Transport;
using Microsoft.Extensions.Options;

namespace LightMQ.IntegrationTest;

public class SqlServerProviderTests : IAsyncLifetime
{
    private readonly SqlServerStorageProvider _sqlseverStorageProvider;
    private readonly IOptions<SqlServerOptions> _sqlserverOptions;
    private readonly IOptions<LightMQOptions> _options;
    private readonly SqlConnection _connection;

    public SqlServerProviderTests()
    {
        _options = Microsoft.Extensions.Options.Options.Create(new LightMQOptions()
        {
        });
        _sqlserverOptions = Microsoft.Extensions.Options.Options.Create(new SqlServerOptions()
        {
            ConnectionString = Environment.GetEnvironmentVariable("APP_MSSQL_CONNECTIONSTRING")!,
        });
        _sqlseverStorageProvider = new SqlServerStorageProvider(_options, _sqlserverOptions);
        _connection = new SqlConnection(_sqlserverOptions.Value.ConnectionString);
        _connection.Open();
    }

    public Task InitializeAsync()
    {
        return _sqlseverStorageProvider.InitTables(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        // 删除_options.Value.TableName表所有数据
        var cmd = _connection.CreateCommand();
        cmd.CommandText = $"DELETE FROM {_options.Value.TableName}";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task PublishNewMessageAsync_ShouldInsertMessageIntoDatabase()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Header = "TestHeader",
            Queue = "TestQueue"
        };

        // Act
        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Assert
        var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
        cmd.Parameters.Add(new SqlParameter("@Id", message.Id));

        var count = (int)await cmd.ExecuteScalarAsync();
        Assert.Equal(1, count); // 验证数据库中是否插入了这条消息
    }

    [Fact]
    public async Task ClearOldMessagesAsync_ShouldDeleteOldMessages()
    {
        // Arrange
        var oldMessage = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "OldTopic",
            Data = "OldData",
            CreateTime = DateTime.Now.AddDays(-8), // 设置为旧消息
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "OldHeader",
            Queue = "OldQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(oldMessage, CancellationToken.None);

        // Act
        await _sqlseverStorageProvider.ClearOldMessagesAsync(CancellationToken.None);

        // Assert
        var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
        cmd.Parameters.Add(new SqlParameter("@Id", oldMessage.Id));

        var count = (int?)await cmd.ExecuteScalarAsync();
        Assert.Equal(0, count); // 验证旧消息是否被删除
    }

    [Fact]
    public async Task NackMessageAsync_ShouldUpdateMessageStatusToFailed()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "NackTopic",
            Data = "NackData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "NackHeader",
            Queue = "NackQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _sqlseverStorageProvider.NackMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText = $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqlParameter("@Id", message.Id));

        var status = (int?)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((int)MessageStatus.Failed, status); // 验证状态是否更新为 Failed
    }

    [Fact]
    public async Task ResetMessageAsync_ShouldUpdateMessageStatusToWaiting()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "ResetTopic",
            Data = "ResetData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Failed,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "ResetHeader",
            Queue = "ResetQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _sqlseverStorageProvider.ResetMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText = $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqlParameter("@Id", message.Id));

        var status = (int?)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((int)MessageStatus.Waiting, status); // 验证状态是否更新为 Waiting
    }

    [Fact]
    public async Task UpdateRetryInfoAsync_ShouldUpdateMessageRetryInfo()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "RetryTopic",
            Data = "RetryData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "RetryHeader",
            Queue = "RetryQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        message.RetryCount = 1; // 更新重试次数
        message.ExecutableTime = DateTime.Now.AddMinutes(10); // 更新可执行时间
        await _sqlseverStorageProvider.UpdateRetryInfoAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT RetryCount, ExecutableTime FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqlParameter("@Id", message.Id));

        using var reader = await updatedMessage.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            Assert.Equal(1, reader.GetInt32(0)); // 验证重试次数是否更新
            var span = message.ExecutableTime - reader.GetDateTime(1);
            Assert.True(span < TimeSpan.FromSeconds(0.01)); // 验证可执行时间是否更新
        }
    }

    [Fact]
    public async Task ResetOutOfDateMessagesAsync_ShouldUpdateOutOfDateMessages()
    {
        // Arrange
        var oldMessage = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "OldTopic",
            Data = "OldData",
            CreateTime = DateTime.Now.AddDays(-1), // 设置为过期消息
            Status = MessageStatus.Processing,
            ExecutableTime = DateTime.Now.AddMinutes(-10), // 设置为过期
            RetryCount = 0,
            Header = "OldHeader",
            Queue = "OldQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(oldMessage, CancellationToken.None);

        // Act
        await _sqlseverStorageProvider.ResetOutOfDateMessagesAsync(CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText = $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqlParameter("@Id", oldMessage.Id));

        var status = (int?)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((int)MessageStatus.Waiting, status); // 验证状态是否更新为 Waiting
    }

    [Fact]
    public async Task PollNewMessageAsync_ShouldReturnAndUpdateMessageStatus()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "PollTopic",
            Data = "PollData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Header = "PollHeader",
            Queue = "PollQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        var polledMessage = await _sqlseverStorageProvider.PollNewMessageAsync("PollTopic", CancellationToken.None);

        // Assert
        Assert.NotNull(polledMessage);
        Assert.Equal(message.Id, polledMessage.Id);
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText = $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqlParameter("@Id", message.Id));

        var status = (int?)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((int)MessageStatus.Processing, status); // 验证状态是否更新为 Processing
    }

    [Fact]
    public async Task PollAllQueuesAsync_ShouldReturnDistinctQueues()
    {
        // Arrange
        var topic = "TestTopic";

        // 插入多个消息到不同的队列
        var message1 = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "Data1",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Header = "Header1",
            Queue = "Queue1"
        };

        var message2 = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "Data2",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Header = "Header2",
            Queue = "Queue2"
        };

        var message3 = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = "Data3",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Header = "Header3",
            Queue = "Queue1" // 重复队列
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message1, CancellationToken.None);
        await _sqlseverStorageProvider.PublishNewMessageAsync(message2, CancellationToken.None);
        await _sqlseverStorageProvider.PublishNewMessageAsync(message3, CancellationToken.None);

        // Act
        var queues = await _sqlseverStorageProvider.PollAllQueuesAsync(topic, CancellationToken.None);

        // Assert
        Assert.NotNull(queues);
        Assert.Equal(2, queues.Count); // 应该返回两个不同的队列
        Assert.Contains("Queue1", queues);
        Assert.Contains("Queue2", queues);
    }

    [Fact]
    public async Task AckMessageAsync_ShouldUpdateMessageStatusToSuccess()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "AckTopic",
            Data = "AckData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Processing,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "AckHeader",
            Queue = "AckQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _sqlseverStorageProvider.AckMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText = $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqlParameter("@Id", message.Id));

        var status = (int?)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((int)MessageStatus.Success, status); // 验证状态是否更新为 Success
    }

    [Fact]
    public async Task PublishNewMessageAsync_WithTransaction_ShouldInsertMessageIntoDatabase()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "TestTopic",
            Data = "TestData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "TestHeader",
            Queue = "TestQueue"
        };

        using var connection = new SqlConnection(_sqlserverOptions.Value.ConnectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        // Act
        await _sqlseverStorageProvider.PublishNewMessageAsync(message, transaction, CancellationToken.None);
        await transaction.CommitAsync();

        // Assert
        var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
        cmd.Parameters.Add(new SqlParameter("@Id", message.Id));

        var count = (int)await cmd.ExecuteScalarAsync();
        Assert.Equal(1, count); // 验证数据库中是否插入了这条消息
    }

    [Fact]
    public async Task PollNewMessageAsync_ShouldReturnAndUpdateMessageStatus2()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "PollTopic",
            Data = "PollData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now,
            RetryCount = 0,
            Header = "PollHeader",
            Queue = "PollQueue"
        };

        await _sqlseverStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        var polledMessage =
            await _sqlseverStorageProvider.PollNewMessageAsync("PollTopic", "PollQueue", CancellationToken.None);

        // Assert
        Assert.NotNull(polledMessage);
        Assert.Equal(message.Id, polledMessage.Id); // 验证返回的消息 ID 是否匹配

        // 验证消息状态是否更新为 Processing
        var updatedMessageCmd = _connection.CreateCommand();
        updatedMessageCmd.CommandText = $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessageCmd.Parameters.Add(new SqlParameter("@Id", message.Id));

        var status = (int)await updatedMessageCmd.ExecuteScalarAsync();
        Assert.Equal((int)MessageStatus.Processing, status); // 验证状态是否更新为 Processing
    }

    [Fact]
    public async Task PublishNewMessagesAsync_ShouldInsertMultipleMessages()
    {
        // Arrange
        var messages = new List<Message>
        {
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic1",
                Data = "TestData1",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now.AddMinutes(5),
                RetryCount = 0,
                Header = "TestHeader1",
                Queue = "TestQueue1"
            },
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic2",
                Data = "TestData2",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now.AddMinutes(5),
                RetryCount = 0,
                Header = "TestHeader2",
                Queue = "TestQueue2"
            }
        };

        // Act
        await _sqlseverStorageProvider.PublishNewMessagesAsync(messages);

        // Assert
        foreach (var message in messages)
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
            cmd.Parameters.Add(new SqlParameter("@Id", message.Id));

            var count = (int)await cmd.ExecuteScalarAsync();
            Assert.Equal(1, count); // 验证每条消息是否插入成功
        }
    }

    [Fact]
    public async Task PublishNewMessagesAsync_WithTransaction_ShouldInsertMultipleMessages()
    {
        // Arrange
        var messages = new List<Message>
        {
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic1",
                Data = "TestData1",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now.AddMinutes(5),
                RetryCount = 0,
                Header = "TestHeader1",
                Queue = "TestQueue1"
            },
            new Message
            {
                Id = Guid.NewGuid().ToString(),
                Topic = "TestTopic2",
                Data = "TestData2",
                CreateTime = DateTime.Now,
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now.AddMinutes(5),
                RetryCount = 0,
                Header = "TestHeader2",
                Queue = "TestQueue2"
            }
        };

        using var connection = new SqlConnection(_sqlserverOptions.Value.ConnectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        // Act
        await _sqlseverStorageProvider.PublishNewMessagesAsync(messages, transaction);

        // Assert
        foreach (var message in messages)
        {
            var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
            cmd.Parameters.Add(new SqlParameter("@Id", message.Id));

            var count = (int?)await cmd.ExecuteScalarAsync();
            Assert.Equal(1, count); // 验证每条消息是否插入成功
        }

        await transaction.CommitAsync();
    }
}