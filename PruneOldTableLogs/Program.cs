using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.ApplicationInsights.WorkerService;

var host = new HostBuilder()

   .ConfigureFunctionsWorkerDefaults(worker =>
   {
       worker.UseTimer(); // This registers the Timer extension
   })
   .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
    })
    .Build();

host.Run();
