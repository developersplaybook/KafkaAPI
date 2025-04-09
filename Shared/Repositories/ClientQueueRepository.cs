using Confluent.Kafka;
using Shared.Interfaces;
using Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Shared.Repositories;
public class ClientQueueRepository : IClientQueueRepository, IDisposable
{
    private readonly string _bootstrapServers = "host.docker.internal:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";

    private readonly List<QueueEntity> _receivedClientMessages = new(); // Simple buffer
    private static readonly SemaphoreSlim _clientSemaphore = new(1, 1);
    private static int _isClientConsuming = 0;
    private readonly CancellationTokenSource _clientCts = new();
    private static readonly object _clientLock = new(); // To protect _receivedMessages

    private readonly IConsumer<Ignore, string> _clientConsumer;

    public ClientQueueRepository()
    {
        var clientConfig = new ConsumerConfig
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

        _clientConsumer = new ConsumerBuilder<Ignore, string>(clientConfig).Build();
        _clientConsumer.Subscribe(_serverQueueTopic);
    }

    public async Task<int> AddClientQueueItemAsync(QueueEntity entity)
    {
        return await ProduceClientMessageAsync(entity);
    }

    public async Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId)
    {
        // Start consuming if it's not already running
        if (Interlocked.CompareExchange(ref _isClientConsuming, 1, 0) == 0)
        {
            _ = Task.Run(() => ConsumeClientLoopAsync());
        }

        await _clientSemaphore.WaitAsync();
        try
        {
            // Wait until the message is found in _receivedMessages
            while (true)
            {
                if (_receivedClientMessages.Count == 0)
                {
                    Task.Delay(10).Wait();
                    continue;
                }

                var match = _receivedClientMessages.FirstOrDefault(x => x?.CorrelationId == correlationId);
                if (match != null)
                {
                    // Remove the message from the list when matched
                    _receivedClientMessages.Remove(match);
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
            _clientSemaphore.Release();
        }
    }

    private async Task ConsumeClientLoopAsync()
    {
        try
        {
            while (!_clientCts.Token.IsCancellationRequested)
            {
                var consumeResult = _clientConsumer.Consume(TimeSpan.FromSeconds(1));

                if (consumeResult?.Message == null)
                {
                    await Task.Delay(10, _clientCts.Token);
                    continue;
                }

                try
                {
                    var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);
                    if (entity != null)
                    {
                        lock (_clientLock)
                        {
                            _receivedClientMessages.Add(entity);
                        }
                    }

                    _clientConsumer.Commit(consumeResult); // Acknowledge the message
                }
                catch (JsonException jex)
                {
                    Console.WriteLine($"JSON parse error: {jex.Message}");
                    // Here you can log/send to "dead letter queue" or similar
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

    private async Task<int> ProduceClientMessageAsync(QueueEntity entity)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.Leader,  // Faster delivery
            LingerMs = 0,        // Reduce delay before message is sent
            BatchNumMessages = 1
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var jsonMessage = JsonSerializer.Serialize(entity);

        try
        {
            var result = await producer.ProduceAsync(_clientQueueTopic, new Message<Null, string> { Value = jsonMessage });
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
        _clientConsumer.Close();
        _clientConsumer.Dispose();
    }
}
