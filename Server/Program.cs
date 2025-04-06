using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
using Shared.Interfaces;
using Shared.Repositories;
using System;
using System.Globalization;
using System.Linq;
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

        await CreateTopic("client-queue");
        await CreateTopic("server-queue");
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
               services.AddSingleton<IServerQueueRepository, ServerQueueRepository>();

               services.AddHostedService<MessageHubService>();
               services.AddTransient<IServerMessageHub, ServerMessageHub>();
           });
    }

    static async Task CreateTopic(string topic)
    {
        string bootstrapServers = "host.docker.internal:9092";

        using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            // 🔍 Kolla om topic redan finns
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            bool topicExists = metadata.Topics.Any(t => t.Topic == topic);

            if (topicExists)
            {
                Console.WriteLine($"✅ Topic '{topic}' finns redan.");
            }
            else
            {
                Console.WriteLine($"⚠️ Topic '{topic}' saknas. Skapar den...");

                // 🛠 Skapa topic
                var topicSpecification = new TopicSpecification
                {
                    Name = topic,
                    NumPartitions = 3,  // Antal partitioner
                    ReplicationFactor = 1 // Replikationsfaktor
                };

                try
                {
                    await adminClient.CreateTopicsAsync(new[] { topicSpecification });
                    Console.WriteLine($"✅ Topic '{topic}' skapades.");
                }
                catch (CreateTopicsException e)
                {
                    Console.WriteLine($"❌ Kunde inte skapa topic: {e.Results[0].Error.Reason}");
                }
            }
        }
    }
}

