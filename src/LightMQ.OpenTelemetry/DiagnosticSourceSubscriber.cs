using System.Diagnostics;
using LightMQ.Diagnostics;

namespace LightMQ.OpenTelemetry;

/// <summary>
/// 监听DiagnosticListener的变化，当有新的DiagnosticListener被创建时，订阅其发布的事件
/// </summary>
internal class DiagnosticSourceSubscriber: IDisposable, IObserver<DiagnosticListener>
{
    private readonly DiagnosticSubscriber _diagnosticSubscriber;

    private long _disposed;
    
    private readonly  List<IDisposable> _listenerSubscriptions;
    
    private IDisposable? _allSourcesSubscription;
    public DiagnosticSourceSubscriber(DiagnosticSubscriber diagnosticSubscriber)
    {
        _diagnosticSubscriber = diagnosticSubscriber;
        _listenerSubscriptions = new List<IDisposable>();
    }
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public void OnCompleted()
    {
    }

    public void OnError(Exception error)
    {
    }

    /// <summary>
    /// 当出现新的DiagnosticListener，要订阅其发出的遥测数据
    /// </summary>
    /// <param name="value"></param>
    public void OnNext(DiagnosticListener value)
    {
        // 没有被销毁且目标的事件监听者的名称必须和 DiagnosticsListenserNames.DiagnosticListenerName一致
        if (Interlocked.Read(ref _disposed) == 0 && value.Name== DiagnosticsListenserNames.DiagnosticListenerName)
        {
            var subscription = 
                value.Subscribe(_diagnosticSubscriber);
            
            lock (_listenerSubscriptions)
            {
                _listenerSubscriptions.Add(subscription);
            }
        }
    }

    public void Subscribe()
    {
        _allSourcesSubscription ??= System.Diagnostics.DiagnosticListener.AllListeners.Subscribe(this);
    }
    
    protected virtual void Dispose(bool disposing)
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) == 1) return;

        lock (_listenerSubscriptions)
        {
            foreach (var listenerSubscription in _listenerSubscriptions)
            {
                listenerSubscription?.Dispose();
            }

            _listenerSubscriptions.Clear();
        }

        _allSourcesSubscription?.Dispose();
        _allSourcesSubscription = null;
    }
}