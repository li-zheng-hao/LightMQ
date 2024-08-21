using System;

namespace LightMQ.BackgroundService;

/// <summary>
/// 后台执行的任务
/// </summary>
public interface IBackgroundService
{
    Task ExecuteAsync(CancellationToken cancellationToken);
}
