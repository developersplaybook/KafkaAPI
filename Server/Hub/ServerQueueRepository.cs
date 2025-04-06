using Confluent.Kafka;
using Server.Interfaces;
using Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace Server.Hub;
public class ServerQueueRepository : IServerHubQueueRepository, IDisposable
{
    private readonly string _bootstrapServers = "host.docker.internal:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";

    private readonly List<QueueEntity> _receivedMessages = new(); // Enkel buffert
    private bool _isConsuming;

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
        if (!_isConsuming)
        {
            _isConsuming = true;
            _ = Task.Run(() => ConsumeLoopAsync());
        }

        while (true)
        {
            if (_receivedMessages.Count == 0)
            {
                Task.Delay(10).Wait();
                continue;
            }

            var match = _receivedMessages.FirstOrDefault();
            if (match != null)
            {
                // Ta bort meddelandet från listan när det matchas
                _receivedMessages.Remove(match);
                return match;
            }
        }
    }

    private async Task ConsumeLoopAsync()
    {
        try
        {
            while (true) // Oändlig loop för att kontinuerligt konsumera meddelanden
            {
                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                if (consumeResult?.Message == null)
                {
                    await Task.Delay(10); // Vänta kort innan vi försöker igen
                    continue;
                }

                var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);

                // Lägg till meddelandet i _receivedMessages om det finns
                _receivedMessages.Add(entity);

                _consumer.Commit(consumeResult); // Bekräfta meddelandet
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consume loop error: {ex.Message}");
        }
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

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}
