using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        // Configure TableServiceClient
        string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        services.AddSingleton(new TableServiceClient(connectionString));
    })
    .Build();

await host.RunAsync();
