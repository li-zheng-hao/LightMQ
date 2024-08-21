using LightMQ.OpenTelemetry;

// ReSharper disable once CheckNamespace
namespace OpenTelemetry.Trace;

public static class TracerProviderBuilderExtensions
{
   
    public static TracerProviderBuilder AddLightMQInstrumentation(this TracerProviderBuilder builder)
    {
        if (builder == null) throw new ArgumentNullException(nameof(builder));

        builder.AddSource(DiagnosticSubscriber.SourceName);

        var instrumentation = new LightMQInstrumentation(new DiagnosticSubscriber());

        return builder.AddInstrumentation(() => instrumentation);
    }
}