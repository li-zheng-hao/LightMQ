using LightMQ.Options;
using LightMQ.Storage.Sqlite;
using LightMQ.Transport;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace LightMQ.IntegrationTest;

public class SqliteProviderTests : IAsyncLifetime
{
    private readonly SqliteStorageProvider _sqliteStorageProvider;
    private readonly IOptions<SqliteOptions> _sqliteOptions;
    private readonly IOptions<LightMQOptions> _options;
    private readonly SqliteConnection _connection;

    public SqliteProviderTests()
    {
        _options = Microsoft.Extensions.Options.Options.Create(new LightMQOptions());
        _sqliteOptions = Microsoft.Extensions.Options.Options.Create(
            new SqliteOptions() { ConnectionString = "Data Source=LightMQ_IntegrationTest.db" }
        );
        _sqliteStorageProvider = new SqliteStorageProvider(_options, _sqliteOptions);
        _connection = new SqliteConnection(_sqliteOptions.Value.ConnectionString);
        _connection.Open();
    }

    public Task InitializeAsync()
    {
        return _sqliteStorageProvider.InitTables(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        try
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_options.Value.TableName}";
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception)
        {
            // Ignore errors during cleanup
        }
        finally
        {
            try
            {
                await _connection.CloseAsync();
                await _connection.DisposeAsync();
            }
            catch (Exception)
            {
                // Ignore errors during connection disposal
            }

            // Wait a bit for the file to be released, then try to delete it
            await Task.Delay(100);
            try
            {
                if (File.Exists("LightMQ_IntegrationTest.db"))
                {
                    File.Delete("LightMQ_IntegrationTest.db");
                }
            }
            catch (Exception)
            {
                // Ignore file deletion errors - the file might still be locked
                // The OS will clean it up eventually
            }
        }
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
            Queue = "TestQueue",
        };

        // Act
        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Assert
        var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
        cmd.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var count = (long)await cmd.ExecuteScalarAsync();
        Assert.Equal(1, count);
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
            CreateTime = DateTime.Now.AddDays(-8),
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now.AddMinutes(5),
            RetryCount = 0,
            Header = "OldHeader",
            Queue = "OldQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(oldMessage, CancellationToken.None);

        // Act
        await _sqliteStorageProvider.ClearOldMessagesAsync(CancellationToken.None);

        // Assert
        var cmd = _connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
        cmd.Parameters.Add(new SqliteParameter("@Id", oldMessage.Id));

        var count = (long)await cmd.ExecuteScalarAsync();
        Assert.Equal(0, count);
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
            Queue = "NackQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _sqliteStorageProvider.NackMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var status = (long)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((long)MessageStatus.Failed, status);
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
            Queue = "ResetQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _sqliteStorageProvider.ResetMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var status = (long)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((long)MessageStatus.Waiting, status);
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
            Queue = "RetryQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        message.RetryCount = 1;
        message.ExecutableTime = DateTime.Now.AddMinutes(10);
        await _sqliteStorageProvider.UpdateRetryInfoAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT RetryCount, ExecutableTime FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqliteParameter("@Id", message.Id));

        using var reader = await updatedMessage.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            Assert.Equal(1, reader.GetInt32(0));
            var span = message.ExecutableTime - reader.GetDateTime(1);
            Assert.True(span < TimeSpan.FromSeconds(1));
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
            CreateTime = DateTime.Now.AddDays(-1),
            Status = MessageStatus.Processing,
            ExecutableTime = DateTime.Now.AddMinutes(-10),
            RetryCount = 0,
            Header = "OldHeader",
            Queue = "OldQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(oldMessage, CancellationToken.None);

        // Act
        await _sqliteStorageProvider.ResetOutOfDateMessagesAsync(
            "OldTopic",
            DateTime.Now,
            CancellationToken.None
        );

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqliteParameter("@Id", oldMessage.Id));

        var status = (long)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((long)MessageStatus.Waiting, status);
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
            ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
            Header = "PollHeader",
            Queue = "PollQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        var polledMessage = await _sqliteStorageProvider.PollNewMessageAsync(
            "PollTopic",
            CancellationToken.None
        );

        // Assert
        Assert.NotNull(polledMessage);
        Assert.Equal(message.Id, polledMessage.Id);
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var status = (long)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((long)MessageStatus.Processing, status);
    }

    [Fact]
    public async Task PollAllQueuesAsync_ShouldReturnDistinctQueues()
    {
        // Arrange
        var topic = "TestTopic";

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
            Queue = "Queue1",
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
            Queue = "Queue2",
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
            Queue = "Queue1",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message1, CancellationToken.None);
        await _sqliteStorageProvider.PublishNewMessageAsync(message2, CancellationToken.None);
        await _sqliteStorageProvider.PublishNewMessageAsync(message3, CancellationToken.None);

        // Act
        var queues = await _sqliteStorageProvider.PollAllQueuesAsync(topic, CancellationToken.None);

        // Assert
        Assert.NotNull(queues);
        Assert.Equal(2, queues.Count);
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
            Queue = "AckQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        await _sqliteStorageProvider.AckMessageAsync(message, CancellationToken.None);

        // Assert
        var updatedMessage = _connection.CreateCommand();
        updatedMessage.CommandText =
            $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessage.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var status = (long)await updatedMessage.ExecuteScalarAsync();
        Assert.Equal((long)MessageStatus.Success, status);
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
            Queue = "TestQueue",
        };

        using var connection = new SqliteConnection(_sqliteOptions.Value.ConnectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        // Act
        await _sqliteStorageProvider.PublishNewMessageAsync(
            message,
            transaction,
            CancellationToken.None
        );
        await transaction.CommitAsync();

        // Assert
        var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
        cmd.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var count = (long)await cmd.ExecuteScalarAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task PollNewMessageAsync_WithQueue_ShouldReturnAndUpdateMessageStatus()
    {
        // Arrange
        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = "PollTopic",
            Data = "PollData",
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            ExecutableTime = DateTime.Now.AddMinutes(-1),
            RetryCount = 0,
            Header = "PollHeader",
            Queue = "PollQueue",
        };

        await _sqliteStorageProvider.PublishNewMessageAsync(message, CancellationToken.None);

        // Act
        var polledMessage = await _sqliteStorageProvider.PollNewMessageAsync(
            "PollTopic",
            "PollQueue",
            CancellationToken.None
        );

        // Assert
        Assert.NotNull(polledMessage);
        Assert.Equal(message.Id, polledMessage.Id);

        var updatedMessageCmd = _connection.CreateCommand();
        updatedMessageCmd.CommandText =
            $"SELECT Status FROM {_options.Value.TableName} WHERE Id = @Id";
        updatedMessageCmd.Parameters.Add(new SqliteParameter("@Id", message.Id));

        var status = (long)await updatedMessageCmd.ExecuteScalarAsync();
        Assert.Equal((long)MessageStatus.Processing, status);
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
                Queue = "TestQueue1",
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
                Queue = "TestQueue2",
            },
        };

        // Act
        await _sqliteStorageProvider.PublishNewMessagesAsync(messages);

        // Assert
        foreach (var message in messages)
        {
            var cmd = _connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
            cmd.Parameters.Add(new SqliteParameter("@Id", message.Id));

            var count = (long)await cmd.ExecuteScalarAsync();
            Assert.Equal(1, count);
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
                Queue = "TestQueue1",
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
                Queue = "TestQueue2",
            },
        };

        using var connection = new SqliteConnection(_sqliteOptions.Value.ConnectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        // Act
        await _sqliteStorageProvider.PublishNewMessagesAsync(messages, transaction);
        await transaction.CommitAsync();

        // Assert
        foreach (var message in messages)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM {_options.Value.TableName} WHERE Id = @Id";
            cmd.Parameters.Add(new SqliteParameter("@Id", message.Id));

            var count = (long)await cmd.ExecuteScalarAsync();
            Assert.Equal(1, count);
        }
    }
}
