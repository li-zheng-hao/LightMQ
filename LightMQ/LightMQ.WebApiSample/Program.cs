using LightMQ.Storage.MongoDB.MongoMQ;
using LightMQ.Storage.SqlServer;
using LightMQ.WebApiSample;
using SW.Core.MongoMQ;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddLightMQ(it =>
{
    it.UseSqlServer("Data Source=.;Initial Catalog=Test;User ID=sa;Password=Abc12345;");
    // it.UseMongoDB("mongodb://localhost:27018","Test");
});
builder.Services.AddHostedService<TestConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();