namespace LightMQ.Storage.SqlServer;

internal static class EnumExtension
{
    public static T ToEnum<T>(this int source)
    {
        return (T) Enum.ToObject(typeof(T), source);
    }
}