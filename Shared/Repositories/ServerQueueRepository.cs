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

public class ServerQueueRepository : IServerQueueRepository, IDisposable
{
    private readonly string _bootstrapServers = "host.docker.internal:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";

    private readonly List<QueueEntity> _receivedServerMessages = new();
    private readonly SemaphoreSlim _serverSemaphore = new(1, 1);
    private readonly CancellationTokenSource _serverCts = new();
    private readonly object _serverLock = new();

    private readonly IConsumer<Ignore, string> _serverConsumer;
    private readonly IProducer<Null, string> _producer;

    public ServerQueueRepository()
    {
        var serverConfig = new ConsumerConfig
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

        _serverConsumer = new ConsumerBuilder<Ignore, string>(serverConfig).Build();
        _serverConsumer.Subscribe(_clientQueueTopic);

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
        _ = Task.Run(() => ConsumeServerLoopAsync(), _serverCts.Token);
    }

    public async Task<int> AddServerQueueItemAsync(QueueEntity entity)
    {
        var jsonMessage = JsonSerializer.Serialize(entity);

        try
        {
            var result = await _producer.ProduceAsync(_serverQueueTopic,
                new Message<Null, string> { Value = jsonMessage });
            return result.Status == PersistenceStatus.Persisted ? 1 : 0;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Producing message failed: {ex.Message}");
            return 0;
        }
    }

    public async Task<QueueEntity?> GetMessageFromClientQueueAsync()
    {
        // Timeout för att undvika oändlig loop
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        try
        {
            while (!cts.Token.IsCancellationRequested)
            {
                await _serverSemaphore.WaitAsync(cts.Token);
                try
                {
                    lock (_serverLock)
                    {
                        if (_receivedServerMessages.Count > 0)
                        {
                            var message = _receivedServerMessages[0];
                            _receivedServerMessages.RemoveAt(0);
                            return message;
                        }
                    }
                }
                finally
                {
                    _serverSemaphore.Release();
                }

                // Vänta lite innan nästa försök
                await Task.Delay(100, cts.Token);
            }

            Console.WriteLine("Timeout waiting for message from client queue");
            return null;
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Operation cancelled while waiting for client message");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"GetMessageFromClientQueueAsync failed: {ex.Message}");
            return null;
        }
    }

    private async Task ConsumeServerLoopAsync()
    {
        try
        {
            while (!_serverCts.Token.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _serverConsumer.Consume(_serverCts.Token);

                    if (consumeResult?.Message?.Value != null)
                    {
                        try
                        {
                            var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);
                            if (entity != null)
                            {
                                lock (_serverLock)
                                {
                                    _receivedServerMessages.Add(entity);

                                    // Begränsa bufferstorleken för att undvika minnesproblem
                                    if (_receivedServerMessages.Count > 1000)
                                    {
                                        Console.WriteLine("Warning: Message buffer exceeded 1000 items, removing oldest");
                                        _receivedServerMessages.RemoveAt(0);
                                    }
                                }
                            }

                            _serverConsumer.Commit(consumeResult);
                        }
                        catch (JsonException jex)
                        {
                            Console.WriteLine($"JSON parse error: {jex.Message}");
                            // Commit ändå för att undvika att fastna på samma meddelande
                            _serverConsumer.Commit(consumeResult);
                        }
                    }
                }
                catch (ConsumeException cex)
                {
                    Console.WriteLine($"Consume error: {cex.Error.Reason}");
                    await Task.Delay(1000, _serverCts.Token);
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
        _serverCts.Cancel();
        _serverCts.Dispose();

        _serverConsumer?.Close();
        _serverConsumer?.Dispose();

        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();

        _serverSemaphore?.Dispose();
    }
}