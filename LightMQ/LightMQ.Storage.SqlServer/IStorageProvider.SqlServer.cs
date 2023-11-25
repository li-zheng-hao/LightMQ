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
    private readonly DbConnection dbConnection;
    private readonly IOptions<LightMQOptions> _mqOptions;
    private readonly IOptions<SqlServerOptions> _dbOptions;

    public SqlServerStorageProvider(IOptions<LightMQOptions> mqOptions, IOptions<SqlServerOptions> dbOptions)
    {
        _mqOptions = mqOptions;
        _dbOptions = dbOptions;
    }

    public async Task PublishNewMessageAsync(Message message,
        CancellationToken cancellationToken = default)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount)";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        await using var _ = connection.ConfigureAwait(false);
        await connection.ExecuteNonQueryAsync(sql, sqlParams:
            new object[]
            {
                new SqlParameter("@Id", message.Id),
                new SqlParameter("@Topic", message.Topic),
                new SqlParameter("@Data", message.Data),
                new SqlParameter("@CreateTime", message.CreateTime),
                new SqlParameter("@Status", message.Status),
                new SqlParameter("@ExecutableTime", message.ExecutableTime),
                new SqlParameter("@RetryCount", message.RetryCount),
            }).ConfigureAwait(false);
    }

    public async Task PublishNewMessageAsync(Message message, object transaction,
        CancellationToken cancellationToken = default)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount)";
        var dbTransaction = transaction as SqlTransaction;
        var connection = dbTransaction.Connection;
        await connection.ExecuteNonQueryAsync(sql, dbTransaction, sqlParams:
            new object[]
            {
                new SqlParameter("@Id", message.Id),
                new SqlParameter("@Topic", message.Topic),
                new SqlParameter("@Data", message.Data),
                new SqlParameter("@CreateTime", message.CreateTime),
                new SqlParameter("@Status", message.Status),
                new SqlParameter("@ExecutableTime", message.ExecutableTime),
                new SqlParameter("@RetryCount", message.RetryCount),
            }).ConfigureAwait(false);
    }

    public Task ClearOldMessagesAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"delete from {_mqOptions.Value.TableName} where CreateTime<=@CreateTime";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql,
            sqlParams: new object[]
                { new SqlParameter("@CreateTime", DateTime.Now.Subtract(_mqOptions.Value.MessageExpireDuration)) });
    }

    public Task NackMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql,
            sqlParams: new object[]
                { new SqlParameter("@Status", MessageStatus.Failed), new SqlParameter("@Id", message.Id) });
    }

    public Task ResetMessageAsync(Message message, CancellationToken cancellationToken = default)
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql,
            sqlParams: new object[]
                { new SqlParameter("@Status", MessageStatus.Waiting), new SqlParameter("@Id", message.Id) });
    }

    public Task UpdateRetryInfoAsync(Message message, CancellationToken cancellationToken = default)
    {
        var sql =
            $"update {_mqOptions.Value.TableName} set Status=@Status,ExecutableTime=@ExecutableTime,RetryCount=@RetryCount where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", message.ExecutableTime),
                new SqlParameter("@RetryCount", message.RetryCount),
                new SqlParameter("@Id", message.Id)
            });
    }

    public Task ResetOutOfDateMessagesAsync(CancellationToken cancellationToken = default)
    {
        var sql =
            $"update {_mqOptions.Value.TableName} set Status=@Status where CreateTime<=@CreateTime and Status=@StatusOrigin";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql,
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Waiting),
                new SqlParameter("@CreateTime", DateTime.Now.Subtract(_mqOptions.Value.MessageTimeoutDuration)),
                new SqlParameter("@StatusOrigin", MessageStatus.Processing)
            });
    }

    public Task<Message?> PollNewMessageAsync(string topic, CancellationToken cancellationToken = default)
    {
        // 将一条消息的状态从Waiting改为Processing，并返回这条消息
        var sql =
            @$"UPDATE top(1) {_mqOptions.Value.TableName} set Status=@Status 
 output inserted.Id,inserted.Topic,inserted.Data,inserted.CreateTime,inserted.Status,inserted.ExecutableTime,inserted.RetryCount  where Topic=@Topic and Status=@StatusOrigin 
 and ExecutableTime<=@ExecutableTime";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteReaderAsync(sql,
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
                        RetryCount = reader.GetInt32(6)
                    };
                }

                return null;
            },
            sqlParams: new object[]
            {
                new SqlParameter("@Status", MessageStatus.Processing),
                new SqlParameter("@Topic", topic),
                new SqlParameter("@StatusOrigin", MessageStatus.Waiting),
                new SqlParameter("@ExecutableTime", DateTime.Now)
            });
    }

    public Task AckMessageAsync(Message currentMessage, CancellationToken stoppingToken = default)
    {
        var sql = $"update {_mqOptions.Value.TableName} set Status=@Status where Id=@Id";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql,
            sqlParams: new object[]
                { new SqlParameter("@Status", MessageStatus.Success), new SqlParameter("@Id", currentMessage.Id) });
    }

    public Task InitTables(CancellationToken stoppingToken = default)
    {
        var sql =
            $"if not exists(select * from sysobjects where name='{_mqOptions.Value.TableName}') create table {_mqOptions.Value.TableName}(" +
            "Id varchar(50) primary key," +
            "Topic nvarchar(255) not null," +
            "Data nvarchar(max) not null," +
            "CreateTime datetime2 not null," +
            "Status int not null," +
            "ExecutableTime datetime2 not null,"+
            "RetryCount int not null"+
            ")";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);
        return connection.ExecuteNonQueryAsync(sql);
    }

    public Task PublishNewMessagesAsync(List<Message> messages)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount)";
        var connection = new SqlConnection(_dbOptions.Value.ConnectionString);  
        return connection.ExecuteAsync(sql, messages);

    }

    public Task PublishNewMessagesAsync(List<Message> messages, object transaction)
    {
        var sql =
            $"insert into {_mqOptions.Value.TableName} (Id,Topic,Data,CreateTime,Status,ExecutableTime,RetryCount) values (@Id,@Topic,@Data,@CreateTime,@Status,@ExecutableTime,@RetryCount)";
        var dbTransaction = transaction as SqlTransaction;
        var connection = dbTransaction.Connection;
        return connection.ExecuteAsync(sql, messages, dbTransaction);
    }
}