using Confluent.Kafka;
using Server.Interfaces;
using Shared.Models;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace Server.Hub;
public class ServerQueueRepository : IServerHubQueueRepository, IDisposable
{
    private readonly string _bootstrapServers = "host.docker.internal:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";
    private readonly IConsumer<Ignore, string> _consumer;

    public ServerQueueRepository()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "queue-group",
            GroupInstanceId = "serverGroupInstanceId",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false,
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 45000,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        _consumer.Subscribe(_clientQueueTopic);
    }

    public async Task<int> AddServerQueueItemAsync(QueueEntity entity)
    {
        return await ProduceMessageAsync(_serverQueueTopic, entity);
    }

    public async Task<QueueEntity?> GetMessageFromClientQueueAsync()
    {
        return await ConsumeMessageAsync(_clientQueueTopic);
    }

    private async Task<int> ProduceMessageAsync(string topic, QueueEntity entity)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.Leader,  // Snabbare leverans
            LingerMs = 5,        // Minska fördröjning innan meddelandet skickas
            BatchNumMessages = 10 // Skickar i batchar
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var jsonMessage = JsonSerializer.Serialize(entity);

        try
        {
            var result = await producer.ProduceAsync(topic, new Message<Null, string> { Value = jsonMessage });
            return result.Status == PersistenceStatus.Persisted ? 1 : 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Producing message failed: {ex.Message}");
            return 0;
        }
    }

    private async Task<QueueEntity?> ConsumeMessageAsync(string topic)
    {
        try
        {
            while (true)
            {
                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1)); // Vänta 1 sekund

                if (consumeResult?.Message == null)
                {
                    await Task.Delay(100);  // Vänta 100ms för att minska CPU-belastning
                    continue;
                }

                var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);
                _consumer.Commit(consumeResult); // Manuell commit av offset
                return entity;
            }
        }
        catch (KafkaException kafkaEx) when (kafkaEx.Error.IsFatal)
        {
            // If topic does not exist or another fatal error occurs, return null
            Console.WriteLine($"Topic not found or fatal error: {kafkaEx.Message}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consuming message failed: {ex.Message}");
            return null;
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}
