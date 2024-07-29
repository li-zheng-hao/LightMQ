using System.Data.SqlClient;
using System.Diagnostics;
using LightMQ.Publisher;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Driver;

namespace LightMQ.WebApiSample.Controllers;

[ApiController]
[Route("[controller]")]
public class WeatherForecastController : ControllerBase
{
    private static readonly string[] Summaries = new[]
    {
        "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
    };

    private readonly ILogger<WeatherForecastController> _logger;

    public WeatherForecastController(ILogger<WeatherForecastController> logger)
    {
        _logger = logger;
    }

    [HttpGet(Name = "GetWeatherForecast")]
    public IEnumerable<WeatherForecast> Get()
    {
        return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
    }

    [HttpPost]
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
}