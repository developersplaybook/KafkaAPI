using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Server.Hub;
using Server.Interfaces;
using Server.Services;
using Shared.DAL;
using Shared.Repositories;
using System.Globalization;
using System.Threading.Tasks;

namespace Server;

internal class Program
{
    public static async Task Main(string[] args)
    {
        CultureInfo.CurrentUICulture = new CultureInfo("en-US");
        var builder = new ConfigurationBuilder()
          .AddJsonFile("appsettings.json");

        var configuration = builder.Build();
        var app = CreateHostBuilder(args, configuration).Build();

        using (var scope = app.Services.GetService<IServiceScopeFactory>().CreateScope())
        {
            scope.ServiceProvider.GetRequiredService<CarApiDbContext>().EnsureSeedData();
        }

        await app.RunAsync();
    }

    static IHostBuilder CreateHostBuilder(string[] args, IConfiguration configuration)
    {
        return Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logging =>
            {
                logging.AddConsole();
            })
           .ConfigureServices(services =>
           {
               services.AddDbContext<CarApiDbContext>(options =>
                    options.UseSqlServer(configuration.GetConnectionString("CarDbConnection")));

               services.AddTransient<ICarRepository, CarRepository>();
               services.AddTransient<ICompanyRepository, CompanyRepository>();
               services.AddTransient<IQueueRepository, QueueRepository>();

               services.AddHostedService<MessageHubService>();
               services.AddTransient<IServerMessageHub, ServerMessageHub>();
           });
    }
}

