using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace LightMQ.Storage.SqlServer;

internal static class DbConnectionExtensions
{
    public static async Task<int> ExecuteNonQueryAsync(this DbConnection connection, string sql,
        DbTransaction? transaction = null, params object[] sqlParams)
    {
        if (connection.State == ConnectionState.Closed) await connection.OpenAsync().ConfigureAwait(false);
        var command = connection.CreateCommand();
        await using var _ = command.ConfigureAwait(false);
        command.CommandType = CommandType.Text;
        command.CommandText = sql;
        foreach (var param in sqlParams)
        {
            command.Parameters.Add(param);
        }

        if (transaction != null) 
            command.Transaction = transaction;

        return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    public static async Task<T> ExecuteReaderAsync<T>(this DbConnection connection, string sql,
        Func<DbDataReader, Task<T>>? readerFunc, DbTransaction? transaction = null, params object[] sqlParams)
    {
        if (connection.State == ConnectionState.Closed) await connection.OpenAsync().ConfigureAwait(false);

        var command = connection.CreateCommand();
        await using var _ = command.ConfigureAwait(false);
        command.CommandType = CommandType.Text;
        command.CommandText = sql;
        foreach (var param in sqlParams)
        {
            command.Parameters.Add(param);
        }

        if (transaction != null) command.Transaction = transaction;

        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        T result = default!;
        if (readerFunc != null) result = await readerFunc(reader).ConfigureAwait(false);

        return result;
    }
}