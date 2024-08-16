using System.Data.Common;
using System.Data.SqlClient;

namespace LightMQ.Storage.SqlServer;

internal static class DataReaderExtension
{
    public static string SafeGetString(this DbDataReader reader, int colIndex)
    {
        if(!reader.IsDBNull(colIndex))
            return reader.GetString(colIndex);
        return string.Empty;
    }
}