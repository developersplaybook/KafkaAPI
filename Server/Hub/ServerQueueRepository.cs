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

    private readonly Queue<Guid> _pendingCalls = new();
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
        _pendingCalls.Enqueue(Guid.NewGuid());

        if (!_isConsuming)
        {
            _isConsuming = true;
            _ = Task.Run(() => ConsumeLoopAsync());
        }

        // Vänta tills meddelandet kommer tillbaka
        while (true)
        {
            if (_receivedMessages.Count > 0)
            {
                var match = _receivedMessages.FirstOrDefault();
                if (match != null)
                {
                    _receivedMessages.Remove(match);
                    return match;
                };
            }

            await Task.Delay(100);
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

    private async Task ConsumeLoopAsync()
    {
        try
        {
            while (_pendingCalls.Count > 0)
            {
                foreach (var currentId in _pendingCalls)
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                    if (consumeResult?.Message == null)
                    {
                        await Task.Delay(100);
                        continue;
                    }

                    var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);

                    _consumer.Commit(consumeResult);
                    _receivedMessages.Add(entity);
                    _pendingCalls.Dequeue(); // Klar, ta nästa
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consume loop error: {ex.Message}");
        }
        finally
        {
            _isConsuming = false;
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}
