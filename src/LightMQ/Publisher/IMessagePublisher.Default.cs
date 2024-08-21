using System.Diagnostics;
using LightMQ.Diagnostics;
using LightMQ.Storage;
using LightMQ.Transport;
using Newtonsoft.Json;

namespace LightMQ.Publisher;

public class MessagePublisher : IMessagePublisher
{
    private static DiagnosticListener _diagnosticListener =
        new DiagnosticListener(DiagnosticsListenserNames.DiagnosticListenerName);

    private readonly IStorageProvider _storageProvider;

    public MessagePublisher(IStorageProvider storageProvider)
    {
        _storageProvider = storageProvider;
    }

    public async Task PublishAsync<T>(string topic, T message) where T : class
    {
        var msg=ConstructMessage(topic, null, message);

        TracingBefore(msg);

        await _storageProvider.PublishNewMessageAsync(msg);

        TracingAfter(msg);
    }

    public async Task PublishAsync<T>(string topic, T message, string queue) where T : class
    {
        var msg=ConstructMessage(topic, queue, message);

        TracingBefore(msg);

        await _storageProvider.PublishNewMessageAsync(msg);

        TracingAfter(msg);
    }

    public async Task PublishAsync<T>(string topic, T message, object transaction) where T : class
    {
        var msg=ConstructMessage(topic, null, message);

        TracingBefore(msg);

        await _storageProvider.PublishNewMessageAsync(msg, transaction);

        TracingAfter(msg);
    }

    public async Task PublishAsync<T>(string topic, T message, object transaction, string queue) where T : class
    {
        var msg=ConstructMessage(topic, queue, message);

        TracingBefore(msg);

        await _storageProvider.PublishNewMessageAsync(msg,transaction);

        TracingAfter(msg);
    }

    public async Task PublishAsync<T>(string topic, List<T> message) where T : class
    {
        List<Message> messages = new();

        foreach (var item in message)
        {
            var msg=ConstructMessage(topic, null, item);

            messages.Add(msg);

            TracingBefore(msg);
        }

        await _storageProvider.PublishNewMessagesAsync(messages);

        foreach (var msg in messages)
        {
            TracingAfter(msg);
        }
    }

    public async Task PublishAsync<T>(string topic, List<T> message, object transaction) where T : class
    {
        List<Message> messages = new();
        foreach (var item in message)
        {
            var msg=ConstructMessage(topic, null, item);
            
            messages.Add(msg);

            TracingBefore(msg);
        }

        await _storageProvider.PublishNewMessagesAsync(messages, transaction);
        
        foreach (var msg in messages)
        {
            TracingAfter(msg);
        }
    }

    private Message ConstructMessage<T>(string topic, string? queue, T message)
    {
        string data = message as string ?? JsonConvert.SerializeObject(message);
        var msg = new Message()
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Data = data,
            CreateTime = DateTime.Now,
            Status = MessageStatus.Waiting,
            RetryCount = 0,
            ExecutableTime = DateTime.Now,
            Queue = queue
        };
        return msg;
    }

    #region tracing

    private static void TracingBefore(Message message)
    {
        if (_diagnosticListener.IsEnabled(DiagnosticsListenserNames.BeforePublish))
        {
            _diagnosticListener.Write(DiagnosticsListenserNames.BeforePublish, message);
        }
    }

    private static void TracingAfter(Message message)
    {
        if (_diagnosticListener.IsEnabled(DiagnosticsListenserNames.AfterPublish))
        {
            _diagnosticListener.Write(DiagnosticsListenserNames.AfterPublish, message);
        }
    }

    #endregion
}