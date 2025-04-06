using Client.Interfaces;
using Confluent.Kafka;
using Shared.Models;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using System.Linq;
using System.Diagnostics;

namespace Client.Hub;
public class ClientQueueRepository : IClientHubQueueRepository, IDisposable
{
    private readonly string _bootstrapServers = "host.docker.internal:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";

    private readonly Queue<Guid> _pendingCorrelationIds = new();
    private readonly List<QueueEntity> _receivedMessages = new(); // Enkel buffert
    private bool _isConsuming;

    private readonly IConsumer<Ignore, string> _consumer;

    public ClientQueueRepository()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "host.docker.internal:9092",
            GroupId = "queue-group",
            GroupInstanceId = "clientGroupInstanceId",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = false,
            MaxPollIntervalMs = 300000,
            SessionTimeoutMs = 45000,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        _consumer.Subscribe(_serverQueueTopic);
    }

    public async Task<int> AddClientQueueItemAsync(QueueEntity entity)
    {
        return await ProduceMessageAsync(_clientQueueTopic, entity);
    }

    public async Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId)
    {
        Console.WriteLine($"_isConsuming: {_isConsuming}, _pendingCorrelationIds.Count: {_pendingCorrelationIds.Count}, _receivedMessages.Count: {_receivedMessages.Count}");
        _pendingCorrelationIds.Enqueue(correlationId);

        if (!_isConsuming)
        {
            _isConsuming = true;
            _ = Task.Run(() => ConsumeLoopAsync());
        }

        // Vänta tills meddelandet kommer tillbaka
        while (true)
        {
            if (_receivedMessages.Count>0)
            {
                var match = _receivedMessages.FirstOrDefault(x => x.CorrelationId == correlationId);
                if (match != null)
                {
                    _receivedMessages.Remove(match);
                    return match;
                }
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
            while (_pendingCorrelationIds.Count > 0)
            {
                // Dequeue the next CorrelationId to check
                var currentId = _pendingCorrelationIds.Dequeue();

                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                if (consumeResult?.Message == null)
                {
                    await Task.Delay(100);  // Wait briefly before trying again
                    _pendingCorrelationIds.Enqueue(currentId);  // Re-enqueue the currentId if not processed yet
                    continue;
                }

                var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);

                if (entity?.CorrelationId == currentId)
                {
                    _consumer.Commit(consumeResult);
                    _receivedMessages.Add(entity);
                    // ID is processed and removed (already dequeued, so nothing further to do)
                }
                else
                {
                    // Re-enqueue currentId since the message didn't match yet
                    Console.WriteLine($"Skipping message with ID {entity?.CorrelationId}");
                    _pendingCorrelationIds.Enqueue(currentId);
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
