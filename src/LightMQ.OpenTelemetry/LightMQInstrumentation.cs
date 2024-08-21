using System.Diagnostics;

namespace LightMQ.OpenTelemetry;

internal class LightMQInstrumentation:IDisposable
{
    private readonly DiagnosticSourceSubscriber? _diagnosticSourceSubscriber;

    public LightMQInstrumentation(DiagnosticSubscriber diagnosticSubscriber)
    {
        _diagnosticSourceSubscriber = new DiagnosticSourceSubscriber(diagnosticSubscriber);
        _diagnosticSourceSubscriber.Subscribe();
    }

    public void Dispose()
    {
        _diagnosticSourceSubscriber?.Dispose();
    }
}