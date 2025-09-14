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

    private readonly List<QueueEntity> _receivedClientMessages = new();
    private readonly SemaphoreSlim _clientSemaphore = new(1, 1);
    private readonly CancellationTokenSource _clientCts = new();
    private readonly object _clientLock = new();

    private readonly IConsumer<Ignore, string> _clientConsumer;
    private readonly IProducer<Null, string> _producer; // Återanvänd producer

    public ClientQueueRepository()
    {
        var clientConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
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

        // Skapa en återanvändbar producer
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 0,
            BatchNumMessages = 1
        };
        _producer = new ProducerBuilder<Null, string>(producerConfig).Build();

        // Starta consume-loopen direkt i konstruktorn
        _ = Task.Run(() => ConsumeClientLoopAsync(), _clientCts.Token);
    }

    public async Task<int> AddClientQueueItemAsync(QueueEntity entity)
    {
        var jsonMessage = JsonSerializer.Serialize(entity);

        try
        {
            var result = await _producer.ProduceAsync(_clientQueueTopic,
                new Message<Null, string> { Value = jsonMessage });
            return result.Status == PersistenceStatus.Persisted ? 1 : 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Producing message failed: {ex.Message}");
            return 0;
        }
    }

    public async Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId)
    {
        // Timeout för att undvika oändlig loop
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                await _clientSemaphore.WaitAsync(cts.Token);
                try
                {
                    lock (_clientLock)
                    {
                        var match = _receivedClientMessages.FirstOrDefault(x => x?.CorrelationId == correlationId);
                        if (match != null)
                        {
                            _receivedClientMessages.Remove(match);
                            return match;
                        }
                    }
                }
                finally
                {
                    _clientSemaphore.Release();
                }

                // Vänta lite innan nästa försök
                await Task.Delay(100, cts.Token);
            }

            Console.WriteLine($"Timeout waiting for message with CorrelationId: {correlationId}");
            return null;
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"Operation cancelled while waiting for CorrelationId: {correlationId}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"GetMessageFromServerByCorrelationIdAsync failed: {ex.Message}");
            return null;
        }
    }

    private async Task ConsumeClientLoopAsync()
    {
        try
        {
            while (!_clientCts.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _clientConsumer.Consume(_clientCts.Token);

                    if (consumeResult?.Message?.Value != null)
                    {
                        try
                        {
                            var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);
                            if (entity != null)
                            {
                                lock (_clientLock)
                                {
                                    _receivedClientMessages.Add(entity);

                                    // Begränsa bufferstorleken för att undvika minnesproblem
                                    if (_receivedClientMessages.Count > 1000)
                                    {
                                        Console.WriteLine("Warning: Message buffer exceeded 1000 items, removing oldest");
                                        _receivedClientMessages.RemoveAt(0);
                                    }
                                }
                            }

                            _clientConsumer.Commit(consumeResult);
                        }
                        catch (JsonException jex)
                        {
                            Console.WriteLine($"JSON parse error: {jex.Message}");
                            // Commit ändå för att undvika att fastna på samma meddelande
                            _clientConsumer.Commit(consumeResult);
                        }
                    }
                }
                catch (ConsumeException cex)
                {
                    Console.WriteLine($"Consume error: {cex.Error.Reason}");
                    await Task.Delay(1000, _clientCts.Token);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consume loop cancelled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Fatal consume loop error: {ex.Message}");
            // Här kunde du implementera retry-logik eller alerting
        }
    }

    public void Dispose()
    {
        _clientCts.Cancel();
        _clientCts.Dispose();

        _clientConsumer?.Close();
        _clientConsumer?.Dispose();

        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();

        _clientSemaphore?.Dispose();
    }
}