using System.Data.Common;
using System.Data.SqlClient;
using System.Transactions;
using Dapper;
using LightMQ.Options;
using LightMQ.Transport;
using Microsoft.Extensions.Options;

namespace LightMQ.Storage.SqlServer;

public class SqlServerStorageProvider : IStorageProvider
{
    /// <summary>
    /// 当前节点标识，用于租约记录
    /// </summary>
    private static readonly string NodeId = Guid.NewGuid().ToString("N");

    private readonly DbConnection dbConnection;
    private readonly IOptions<LightMQOptions> _mqOptions;
    private readonly IOptions<SqlServerOptions> _dbOptions;

    public SqlServerStorageProvider(
        IOptions<LightMQOptions> mqOptions,
        IOptions<SqlServerOptions> dbOptions
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
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection
            .ExecuteNonQueryAsync(
                sql,
                sqlParams: new object[]
                {
                    new SqlParameter("@Id", message.Id),
                    new SqlParameter("@Topic", message.Topic),
                    new SqlParameter("@Data", message.Data),
                    new SqlParameter("@CreateTime", message.CreateTime),
                    new SqlParameter("@Status", message.Status),
                    new SqlParameter("@ExecutableTime", message.ExecutableTime),
                    new SqlParameter("@RetryCount", message.RetryCount),
                    new SqlParameter("@Header", message.Header ?? string.Empty),
                    new SqlParameter("@Queue", message.Queue ?? string.Empty),
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
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header,Queue) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header,@Queue)";
        var dbTransaction = transaction as SqlTransaction;
        var connection = dbTransaction.Connection;
        await connection
            .ExecuteNonQueryAsync(
                sql,
                dbTransaction,
                sqlParams: new object[]
                {
                    new SqlParameter("@Id", message.Id),
                    new SqlParameter("@Topic", message.Topic),
                    new SqlParameter("@Data", message.Data),
                    new SqlParameter("@CreateTime", message.CreateTime),
                    new SqlParameter("@Status", message.Status),
                    new SqlParameter("@ExecutableTime", message.ExecutableTime),
                    new SqlParameter("@RetryCount", message.RetryCount),
                    new SqlParameter("@Header", message.Header ?? string.Empty),
                    new SqlParameter("@Queue", message.Queue ?? string.Empty),
                }
            )
            .ConfigureAwait(false);
    }

    public async Task ClearOldMessagesAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"delete from {_mqOptions.Value.TableName} where CreateTime<=@CreateTime";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter(
                    "@CreateTime",
                    DateTime.Now.Subtract(_mqOptions.Value.MessageExpireDuration)
                ),
            }
        );
    }

    public async Task NackMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Failed),
                new SqlParameter("@Id", message.Id),
            }
        );
    }

    public async Task ResetMessageAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Waiting),
                new SqlParameter("@Id", message.Id),
            }
        );
    }

    public async Task UpdateRetryInfoAsync(
        Message message,
        CancellationToken cancellationToken = default
    )
    {
        var sql =
            $"update {_mqOptions.Value.TableName} set Status=@Status,ExecutableTime=@ExecutableTime,RetryCount=@RetryCount where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", message.ExecutableTime),
                new SqlParameter("@RetryCount", message.RetryCount),
                new SqlParameter("@Id", message.Id),
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
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Waiting),
                new SqlParameter("@NowTime", DateTime.Now),
                new SqlParameter("@ExecutableTime", executeTime),
                new SqlParameter("@StatusOrigin", MessageStatus.Processing),
                new SqlParameter("@Topic", topic),
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
            @$"UPDATE top(1) {_mqOptions.Value.TableName} WITH (ROWLOCK, READPAST) set Status=@Status,ExecutableTime=@ExecutableTime
 output inserted.Id,inserted.Topic,inserted.Data,inserted.CreateTime,inserted.Status,inserted.ExecutableTime,inserted.RetryCount,inserted.Header,inserted.Queue  where Topic=@Topic and Status=@StatusOrigin
 and ExecutableTime<=@ExecutableTime";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.ExecuteReaderAsync(
            sql,
            readerFunc: async reader =>
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    return new Message()
                    {
                        Id = reader.GetString(0),
                        Topic = reader.GetString(1),
                        Data = reader.GetString(2),
                        CreateTime = reader.GetDateTime(3),
                        Status = reader.GetInt32(4).ToEnum<MessageStatus>(),
                        ExecutableTime = reader.GetDateTime(5),
                        RetryCount = reader.GetInt32(6),
                        Header = reader.SafeGetString(7),
                        Queue = reader.SafeGetString(8),
                    };
                }

                return null;
            },
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Processing),
                new SqlParameter("@Topic", topic),
                new SqlParameter("@StatusOrigin", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", DateTime.Now),
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
            @$"UPDATE top(1) {_mqOptions.Value.TableName} WITH (ROWLOCK, READPAST) set Status=@Status,ExecutableTime=@ExecutableTime
 output inserted.Id,inserted.Topic,inserted.Data,inserted.CreateTime,inserted.Status,inserted.ExecutableTime,inserted.RetryCount,inserted.Header,inserted.Queue  where Topic=@Topic and Status=@StatusOrigin
 and ExecutableTime<=@ExecutableTime and Queue=@Queue";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.ExecuteReaderAsync(
            sql,
            readerFunc: async reader =>
            {
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    return new Message()
                    {
                        Id = reader.GetString(0),
                        Topic = reader.GetString(1),
                        Data = reader.GetString(2),
                        CreateTime = reader.GetDateTime(3),
                        Status = reader.GetInt32(4).ToEnum<MessageStatus>(),
                        ExecutableTime = reader.GetDateTime(5),
                        RetryCount = reader.GetInt32(6),
                        Header = reader.GetString(7),
                        Queue = reader.GetString(8),
                    };
                }

                return null;
            },
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Processing),
                new SqlParameter("@Topic", topic),
                new SqlParameter("@StatusOrigin", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", DateTime.Now),
                new SqlParameter("@Queue", queue),
            }
        );
    }

    public async Task<List<Message>> PollNewMessagesAsync(
        string topic,
        int count,
        CancellationToken cancellationToken = default
    )
    {
        // 批量将最多 count 条消息的状态从 Waiting 改为 Processing，并返回这些消息
        var sql =
            @$"UPDATE TOP (@Count) {_mqOptions.Value.TableName} WITH (ROWLOCK, READPAST)
 set Status=@Status,ExecutableTime=@ExecutableTime
 output inserted.Id,inserted.Topic,inserted.Data,inserted.CreateTime,inserted.Status,inserted.ExecutableTime,inserted.RetryCount,inserted.Header,inserted.Queue
 where Topic=@Topic and Status=@StatusOrigin
 and ExecutableTime<=@ExecutableTime";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.ExecuteReaderAsync(
            sql,
            readerFunc: async reader =>
            {
                var messages = new List<Message>();
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    messages.Add(
                        new Message()
                        {
                            Id = reader.GetString(0),
                            Topic = reader.GetString(1),
                            Data = reader.GetString(2),
                            CreateTime = reader.GetDateTime(3),
                            Status = reader.GetInt32(4).ToEnum<MessageStatus>(),
                            ExecutableTime = reader.GetDateTime(5),
                            RetryCount = reader.GetInt32(6),
                            Header = reader.SafeGetString(7),
                            Queue = reader.SafeGetString(8),
                        }
                    );
                }

                return messages;
            },
            sqlParams: new object[]
            {
                new SqlParameter("@Count", count),
                new SqlParameter("@Status", MessageStatus.Processing),
                new SqlParameter("@Topic", topic),
                new SqlParameter("@StatusOrigin", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", DateTime.Now),
            }
        );
    }

    public async Task<List<Message>> PollNewMessagesAsync(
        string topic,
        string? queue,
        int count,
        CancellationToken cancellationToken = default
    )
    {
        // 批量将指定队列中最多 count 条消息的状态从 Waiting 改为 Processing，并返回这些消息
        var sql =
            @$"UPDATE TOP (@Count) {_mqOptions.Value.TableName} WITH (ROWLOCK, READPAST)
 set Status=@Status,ExecutableTime=@ExecutableTime
 output inserted.Id,inserted.Topic,inserted.Data,inserted.CreateTime,inserted.Status,inserted.ExecutableTime,inserted.RetryCount,inserted.Header,inserted.Queue
 where Topic=@Topic and Status=@StatusOrigin
 and ExecutableTime<=@ExecutableTime and Queue=@Queue";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        return await connection.ExecuteReaderAsync(
            sql,
            readerFunc: async reader =>
            {
                var messages = new List<Message>();
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    messages.Add(
                        new Message()
                        {
                            Id = reader.GetString(0),
                            Topic = reader.GetString(1),
                            Data = reader.GetString(2),
                            CreateTime = reader.GetDateTime(3),
                            Status = reader.GetInt32(4).ToEnum<MessageStatus>(),
                            ExecutableTime = reader.GetDateTime(5),
                            RetryCount = reader.GetInt32(6),
                            Header = reader.SafeGetString(7),
                            Queue = reader.SafeGetString(8),
                        }
                    );
                }

                return messages;
            },
            sqlParams: new object[]
            {
                new SqlParameter("@Count", count),
                new SqlParameter("@Status", MessageStatus.Processing),
                new SqlParameter("@Topic", topic),
                new SqlParameter("@StatusOrigin", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", DateTime.Now),
                new SqlParameter("@Queue", queue),
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
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await connection.OpenAsync();
        await using var _ = connection.ConfigureAwait(false);
        // 补全下面的代码

        using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@Topic", topic);
        command.Parameters.AddWithValue("@StatusOrigin", MessageStatus.Waiting);
        command.Parameters.AddWithValue("@ExecutableTime", DateTime.Now);

        var queues = new List<string?>();

        await using (
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                queues.Add(reader.SafeGetString(0)); // 获取 Queue 列的值
            }
        }

        return queues;
    }

    public async Task AckMessageAsync(
        Message currentMessage,
        CancellationToken stoppingToken = default
    )
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Success),
                new SqlParameter("@Id", currentMessage.Id),
            }
        );
    }

    public async Task InitTables(CancellationToken stoppingToken = default)
    {
        var indexName = $"IX_{_mqOptions.Value.TableName}_Topic_Status_ExecutableTime";
        if (indexName.Length > 128)
            indexName = indexName[..128];
        var sql = $"""
            IF NOT EXISTS(SELECT * FROM sysobjects WHERE name='{_mqOptions.Value.TableName}') CREATE TABLE {_mqOptions.Value.TableName}(
            Id varchar(50) primary key,
            Topic nvarchar(255) not null,
            Data nvarchar(max) not null,
            CreateTime datetime2 not null,
            Status int not null,
            ExecutableTime datetime2 not null,
            RetryCount int not null,
            Header nvarchar(max)
            )
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS
                           WHERE TABLE_NAME = '{_mqOptions.Value.TableName}' AND COLUMN_NAME = 'Queue')
            BEGIN
                ALTER TABLE {_mqOptions.Value.TableName}
                ADD Queue nvarchar(255)
            END
            IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='{indexName}' AND object_id=OBJECT_ID('{_mqOptions.Value.TableName}'))
            BEGIN
                CREATE NONCLUSTERED INDEX {indexName} ON {_mqOptions.Value.TableName}(Topic, Status, ExecutableTime)
            END
            IF NOT EXISTS(SELECT * FROM sysobjects WHERE name='{_mqOptions.Value.TableName}_lease') CREATE TABLE {_mqOptions.Value.TableName}_lease(
            LeaseKey varchar(50) primary key,
            Owner varchar(50) not null,
            ExpireTime datetime2 not null
            )
            IF NOT EXISTS (SELECT * FROM {_mqOptions.Value.TableName}_lease WHERE LeaseKey='reset')
                INSERT INTO {_mqOptions.Value.TableName}_lease(LeaseKey,Owner,ExpireTime) VALUES('reset','','1900-01-01')
            """;
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(sql);
    }

    public async Task<bool> TryAcquireResetLeaseAsync(TimeSpan duration, CancellationToken cancellationToken = default)
    {
        // 原子更新租约：只有租约已过期或未被持有时才更新成功（受影响行数为1）
        var sql =
            $"update {_mqOptions.Value.TableName}_lease set Owner=@Owner,ExpireTime=@ExpireTime where LeaseKey=@LeaseKey and ExpireTime<=@Now";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        var count = await connection.ExecuteNonQueryAsync(
            sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Owner", NodeId),
                new SqlParameter("@ExpireTime", DateTime.Now.Add(duration)),
                new SqlParameter("@LeaseKey", "reset"),
                new SqlParameter("@Now", DateTime.Now),
            }
        );
        return count > 0;
    }

    public async Task PublishNewMessagesAsync(List<Message> messages)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header,Queue) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header,@Queue)";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        connection.Open();
        await using var transaction = connection.BeginTransaction();
        await connection.ExecuteAsync(sql, messages, transaction);
        await transaction.CommitAsync();
    }

    public Task PublishNewMessagesAsync(List<Message> messages, object transaction)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount,Header,Queue) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount,@Header,@Queue)";
        var dbTransaction = transaction as SqlTransaction;
        var connection = dbTransaction.Connection;
        return connection.ExecuteAsync(sql, messages, dbTransaction);
    }

    public async Task RequeueMessageAsync(Message currentMessage)
    {
        // 将一条消息的状态改为Waiting
        var sql =
            @$"
    UPDATE {_mqOptions.Value.TableName} 
    SET Status = @Status,ExecutableTime=@ExecutableTime
    WHERE Id= @Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteAsync(
            sql,
            new
            {
                Status = MessageStatus.Waiting,
                ExecutableTime = DateTime.Now,
            }
        );
    }
}
