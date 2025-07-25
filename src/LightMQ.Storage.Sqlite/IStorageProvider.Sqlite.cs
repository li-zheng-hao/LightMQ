using Dapper;
using LightMQ.Options;
using LightMQ.Transport;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;

namespace LightMQ.Storage.Sqlite;

public class SqliteStorageProvider : IStorageProvider
{
    private readonly IOptions<LightMQOptions> _mqOptions;
    private readonly IOptions<SqliteOptions> _dbOptions;

    public SqliteStorageProvider(
        IOptions<LightMQOptions> mqOptions,
        IOptions<SqliteOptions> dbOptions
    )
    {
        _mqOptions = mqOptions;
        _dbOptions = dbOptions;
    }

    public async Task PublishNewMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header,Queue) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header,@Queue)";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection
            .ExecuteAsync(
                sql,
                new
                {
                    message.Id,
                    message.Topic,
                    message.Data,
                    message.CreateTime,
                    message.Status,
                    message.ExecutableTime,
                    message.RetryCount,
                    message.Header,
                    message.Queue,
                }
            )
            .ConfigureAwait(false);
    }

    public async Task PublishNewMessageAsync(
        Message message,
        object transaction,
        CancellationToken cancellationToken = default
    )
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header)";
        var dbTransaction = transaction as SqliteTransaction;
        var connection = dbTransaction!.Connection;
        await connection!
            .ExecuteAsync(
                sql,
                new
                {
                    message.Id,
                    message.Topic,
                    message.Data,
                    message.CreateTime,
                    message.Status,
                    message.ExecutableTime,
                    message.RetryCount,
                    Header = message.Header ?? string.Empty,
                    Queue = message.Queue ?? string.Empty,
                },
                dbTransaction
            )
            .ConfigureAwait(false);
    }

    public async Task ClearOldMessagesAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"delete from {_mqOptions.Value.TableName} where CreateTime<=@CreateTime";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(
            sql,
            new { CreateTime = DateTime.Now.Subtract(_mqOptions.Value.MessageExpireDuration) }
        );
    }

    public async Task NackMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(sql, new { Status = MessageStatus.Failed, message.Id });
    }

    public async Task ResetMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(sql, new { Status = MessageStatus.Waiting, message.Id });
    }

    public async Task UpdateRetryInfoAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql =
            $"update {_mqOptions.Value.TableName} set Status=@Status,ExecutableTime=@ExecutableTime,RetryCount=@RetryCount where Id=@Id";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(
            sql,
            new
            {
                Status = MessageStatus.Waiting,
                message.ExecutableTime,
                message.RetryCount,
                message.Id,
            }
        );
    }

    public async Task ResetOutOfDateMessagesAsync(
        string topic,
        DateTime executeTime,
        CancellationToken cancellationToken = default
    )
    {
        var sql =
            $"update {_mqOptions.Value.TableName} set Status=@Status, ExecutableTime=@NowTime where ExecutableTime<=@ExecutableTime and Status=@StatusOrigin and Topic=@Topic";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(
            sql,
            new
            {
                Status = MessageStatus.Waiting,
                NowTime = DateTime.Now,
                ExecutableTime = executeTime,
                StatusOrigin = MessageStatus.Processing,
                Topic = topic,
            }
        );
    }

    public async Task<Message?> PollNewMessageAsync(
        string topic,
        CancellationToken cancellationToken = default
    )
    {
        // 将一条消息的状态从Waiting改为Processing，并返回这条消息
        var sql =
            @$"
    UPDATE {_mqOptions.Value.TableName} 
    SET Status = @Status,ExecutableTime=@ExecutableTime
    WHERE ROWID = (
        SELECT MIN(ROWID) FROM {_mqOptions.Value.TableName} 
        WHERE Topic = @Topic 
        AND Status = @StatusOrigin 
        AND ExecutableTime <= @ExecutableTime 
    )
    RETURNING Id, Topic, Data, CreateTime, Status, ExecutableTime, RetryCount, Header, Queue";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.QueryFirstOrDefaultAsync<Message>(
            sql,
            new
            {
                Status = MessageStatus.Processing,
                Topic = topic,
                StatusOrigin = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
            }
        );
    }

    public async Task<Message?> PollNewMessageAsync(
        string topic,
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        // 将一条消息的状态从Waiting改为Processing，并返回这条消息
        var sql =
            @$"
    UPDATE {_mqOptions.Value.TableName} 
    SET Status = @Status,ExecutableTime=@ExecutableTime
    WHERE ROWID = (
        SELECT MIN(ROWID) FROM {_mqOptions.Value.TableName} 
        WHERE Topic = @Topic 
        AND Status = @StatusOrigin 
        AND ExecutableTime <= @ExecutableTime 
    )
    RETURNING Id, Topic, Data, CreateTime, Status, ExecutableTime, RetryCount, Header, Queue";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.QueryFirstOrDefaultAsync<Message>(
            sql,
            new
            {
                Status = MessageStatus.Processing,
                Topic = topic,
                StatusOrigin = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
                Queue = queue,
            }
        );
    }

    public async Task<List<string?>> PollAllQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default
    )
    {
        var sql =
            @$"SELECT Queue
FROM {_mqOptions.Value.TableName}
where Topic=@Topic and Status=@StatusOrigin 
 and ExecutableTime<=@ExecutableTime
GROUP BY Queue;";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await connection.OpenAsync();
        await using var _ = connection.ConfigureAwait(false);
        var queues = await connection.QueryAsync<string?>(
            sql,
            new
            {
                Topic = topic,
                StatusOrigin = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
            }
        );
        return queues.ToList();
    }

    public async Task AckMessageAsync(
        Message currentMessage,
        CancellationToken stoppingToken = default
    )
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(
            sql,
            new { Status = MessageStatus.Success, currentMessage.Id }
        );
    }

    public async Task InitTables(CancellationToken stoppingToken = default)
    {
        var sql = $"""
            CREATE TABLE IF NOT EXISTS {_mqOptions.Value.TableName} (
                Id TEXT PRIMARY KEY,
                Topic TEXT NOT NULL,
                Data TEXT NOT NULL,
                CreateTime DATETIME NOT NULL,
                Status INTEGER NOT NULL,
                ExecutableTime DATETIME NOT NULL,
                RetryCount INTEGER NOT NULL,
                Header TEXT,
                Queue TEXT
            )
            """;
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(sql);
        // var version=connection.ExecuteScalar<string>("select sqlite_version();");
        // Console.WriteLine($"SQLite 版本: {version}");
    }

    public async Task PublishNewMessagesAsync(List<Message> messages)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header,Queue) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header,@Queue)";
        var connection = new SqliteConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        connection.Open();
        await using var transaction = connection.BeginTransaction();
        await connection.ExecuteAsync(sql, messages, transaction);
        await transaction.CommitAsync();
    }

    public Task PublishNewMessagesAsync(List<Message> messages, object transaction)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header)";
        var dbTransaction = transaction as SqliteTransaction;
        var connection = dbTransaction!.Connection;
        return connection!.ExecuteAsync(sql, messages, dbTransaction);
    }
}
