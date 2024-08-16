using LightMQ;
using LightMQ.Storage.MongoDB.MongoMQ;
using LightMQ.Storage.SqlServer;
using LightMQ.WebApiSample;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddLightMQ(it =>
{
    // it.UseSqlServer(@"Server=localhost\SQLEXPRESS;Database=LightMQTest;Trusted_Connection=True;");
    it.UseMongoDB("mongodb://localhost:27017","LightMQTest");
});

builder.Services.AddScoped<Test2Consumer>();
builder.Services.AddScoped<TestQueueConsumer>();

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