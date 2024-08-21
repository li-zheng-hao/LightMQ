namespace LightMQ.Diagnostics;

/// <summary>
/// Opentelemetery监听事件
/// </summary>
public static class DiagnosticsListenserNames
{
    private const string Prefix = "LightMQ.";

    //Tracing
    public const string DiagnosticListenerName =Prefix+"DiagnosticListener";

    public const string BeforePublish = Prefix + "PublishBefore";
    public const string AfterPublish = Prefix + "PublishAfter";
    public const string BeforeConsume = Prefix + "ConsumeBefore";
    public const string AfterConsume = Prefix + "ConsumeAfter";

    //Metrics
    // public const string MetricListenerName = Prefix + "EventCounter";
    // public const string PublishedPerSec = "published-per-second";
    // public const string ConsumePerSec = "consume-per-second";
    // public const string InvokeSubscriberPerSec = "invoke-subscriber-per-second";
    // public const string InvokeSubscriberElapsedMs = "invoke-subscriber-elapsed-ms";
}