using System.Data.SqlClient;
using System.Diagnostics;
using LightMQ.Publisher;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;

namespace LightMQ.WebApiSample.Controllers;

[ApiController]
[Route("[controller]")]
public class PublishController : ControllerBase
{
    private readonly ILogger<PublishController> _logger;

    public PublishController(ILogger<PublishController> logger)
    {
        _logger = logger;
    }
    [HttpPost("publish-single")]
    public async Task<IActionResult> Publish([FromServices] IMessagePublisher messagePublisher)
    {
        int num = 1;
        // 60秒
        // var cancel = new CancellationTokenSource(60 * 1000);
        // while (!cancel.IsCancellationRequested)
        // {
        //     await messagePublisher.PublishAsync("test", "111");
        //     num++;
        //     Console.WriteLine(num);
        // }
        Stopwatch sw = new();
        sw.Restart();
        
        var data=Enumerable.Range(1, 1).Select(it => it.ToString()).ToList();
        await messagePublisher.PublishAsync<string>("test", data);
        _logger.LogInformation($" {sw.ElapsedMilliseconds}ms");
        return Ok(num);
    }

    [HttpPost("publish-with-queue")]
    public async Task<IActionResult> PublishWithQueue([FromServices] IMessagePublisher messagePublisher)
    {
        Stopwatch sw = new();
        sw.Restart();
        foreach (var it in Enumerable.Range(1, 15).ToList())
        {
            string message;
            if (it % 5 == 0)
            {
                message = $"{it}:queue1";
                await messagePublisher.PublishAsync<string>("test-queue", message,null);
            }
            else if (it % 5 == 1)
            {
                message = $"{it}:queue2";
                await messagePublisher.PublishAsync<string>("test-queue", message, "queue2");
            }
            else if (it % 5== 2)
            {
                message = $"{it}:queue3";
                await messagePublisher.PublishAsync<string>("test-queue", message, "queue3");
            }
            else if (it % 5== 3)
            {
                message = $"{it}:queue4";
                await messagePublisher.PublishAsync<string>("test-queue", message, "queue4");
            }
            else if (it % 5== 4)
            {
                message = $"{it}:queue5";
                await messagePublisher.PublishAsync<string>("test-queue", message, "queue5");
            }
        }
        return Ok(1);
    }

}