using LightMQ;
using LightMQ.Storage.MongoDB.MongoMQ;
using LightMQ.WebApiSample;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddLightMQ(it =>
{
    // it.UseSqlServer("server=localhost;uid=sa;pwd=Abc.12345;database=Test;TrustServerCertificate=true;");
    it.UseMongoDB("","LightMQTest");
});

builder.Services.AddScoped<Test2Consumer>();

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