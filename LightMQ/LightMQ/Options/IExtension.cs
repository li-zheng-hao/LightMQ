using Microsoft.Extensions.DependencyInjection;

namespace LightMQ.Options;

public interface IExtension
{
    IServiceCollection AddExtension(IServiceCollection serviceCollection);
}