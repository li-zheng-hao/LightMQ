using System.Diagnostics;
using LightMQ.Transport;
using Microsoft.VisualBasic;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using LightMQEvents = LightMQ.Diagnostics.DiagnosticsListenserNames;

namespace LightMQ.OpenTelemetry;

/// <summary>
/// 监听LightMQ内部发出的遥测数据
/// </summary>
public class DiagnosticSubscriber : IObserver<KeyValuePair<string, object?>>
{
    public const string SourceName = "LightMQ.OpenTelemetry";

    private static readonly ActivitySource ActivitySource = new(SourceName, "1.0.0");

    private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;


    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    public void OnNext(KeyValuePair<string, object?> evt)
    {
        var eventData = (Message)evt.Value!;
        switch (evt.Key)
        {
            case LightMQEvents.BeforePublish:
            {
                ActivityContext parentContext = Propagator.Extract(default, eventData, (msg, key) =>
                {
                    var header = msg.GetHeader() ?? new Dictionary<string, string>();
                    if (header.TryGetValue(key, out var value)) return new[] { value };
                    return Enumerable.Empty<string>();
                }).ActivityContext;
                if (parentContext == default)
                {
                    parentContext = Activity.Current?.Context ?? default;
                }

                var activity = ActivitySource.StartActivity("LightMQ发送消息 主题：" + eventData.Topic,
                    ActivityKind.Internal, parentContext);
                if (activity != null)
                {
                    activity.SetTag("message.topic", eventData.Topic);
                    activity.SetTag("message.id", eventData.Id);

                    if (parentContext != default && Activity.Current != null)
                    {
                        Propagator.Inject(new PropagationContext(Activity.Current.Context, Baggage.Current),
                            eventData,
                            (msg, key, value) => { msg.AddHeader(key, value); });
                    }

                    ;
                }
            }
                break;
            case LightMQEvents.AfterPublish:
            {
                var elapsedTime = DateTimeOffset.FromUnixTimeSeconds(
                    (long)(DateTime.UtcNow - eventData.CreateTime.ToUniversalTime())
                    .TotalSeconds);
                Activity.Current?.AddEvent(new ActivityEvent("LightMQ发送消息完成", elapsedTime,
                    new ActivityTagsCollection { new("lightmq.duration", elapsedTime) })
                );
                Activity.Current?.Stop();
            }
                break;
            case LightMQEvents.BeforeConsume:
            {
                var header = eventData.GetHeader();
                var parentContext = Propagator.Extract(default, header, (msg, key) =>
                {
                    if (header?.TryGetValue(key, out var value) == true) return new[] { value };
                    return Enumerable.Empty<string>();
                });

                Baggage.Current = parentContext.Baggage;
                var activity = ActivitySource.StartActivity(
                    "LightMQ消费消息 主题："+eventData.Topic,
                    ActivityKind.Consumer,
                    parentContext.ActivityContext);

                if (activity != null)
                {
                    activity.SetTag("message.topic", eventData.Topic);
                    activity.SetTag("message.id", eventData.Id);
                }
            }
                break;
            case LightMQEvents.AfterConsume:
            {
                if (Activity.Current is { } activity)
                {
                    activity.Stop();
                }
            }
                break;
        }
    }
}