using Client.Interfaces;
using Confluent.Kafka;
using Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Client.Hub;
public class ClientQueueRepository : IClientHubQueueRepository, IDisposable
{
    private readonly string _bootstrapServers = "host.docker.internal:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";

    private readonly List<QueueEntity> _receivedMessages = new(); // Enkel buffert
    private static readonly SemaphoreSlim _semaphore = new(1, 1);
    private static int _isConsuming = 0;
    private readonly CancellationTokenSource _cts = new();
    private static readonly object _lock = new(); // För att skydda _receivedMessages

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
        // Starta konsumtion om den inte redan pågår
        if (Interlocked.CompareExchange(ref _isConsuming, 1, 0) == 0)
        {
            _ = Task.Run(() => ConsumeLoopAsync());
        }

        await _semaphore.WaitAsync();
        try
        {
            // Vänta tills meddelandet finns i _receivedMessages
            while (true)
            {
                if (_receivedMessages.Count == 0)
                {
                    Task.Delay(10).Wait();
                    continue;
                }

                var match = _receivedMessages.FirstOrDefault(x => x?.CorrelationId == correlationId);
                if (match != null)
                {
                    // Ta bort meddelandet från listan när det matchas
                    _receivedMessages.Remove(match);
                    return match;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"GetMessageFromServerByCorrelationIdAsync failed: {ex.Message}");
            return null;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private async Task ConsumeLoopAsync()
    {
        try
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                var consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                if (consumeResult?.Message == null)
                {
                    await Task.Delay(10, _cts.Token);
                    continue;
                }

                try
                {
                    var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);
                    if (entity != null)
                    {
                        lock (_lock)
                        {
                            _receivedMessages.Add(entity);
                        }
                    }

                    _consumer.Commit(consumeResult); // Bekräfta meddelandet
                }
                catch (JsonException jex)
                {
                    Console.WriteLine($"JSON parse error: {jex.Message}");
                    // Här kan du logga/skicka till "dead letter queue" eller liknande
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consume loop cancelled.");
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
