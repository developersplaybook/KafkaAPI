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

    private readonly List<QueueEntity> _receivedServerMessages = new(); // Enkel buffert
    private static readonly SemaphoreSlim _serverSemaphore = new(1, 1);
    private static int _isServerConsuming = 0;
    private readonly CancellationTokenSource _serverCts = new();
    private static readonly object _serverLock = new(); // För att skydda _receivedMessages

    private readonly IConsumer<Ignore, string> _serverConsumer;

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
    }

    public async Task<int> AddServerQueueItemAsync(QueueEntity entity)
    {
        return await ProduceServerMessageAsync(entity);
    }

    public async Task<QueueEntity?> GetMessageFromClientQueueAsync()
    {
        // Starta konsumtion om den inte redan pågår
        if (Interlocked.CompareExchange(ref _isServerConsuming, 1, 0) == 0)
        {
            _ = Task.Run(() => ConsumeServerLoopAsync());
        }

        await _serverSemaphore.WaitAsync();
        try
        {
            while (true)
            {
                if (_receivedServerMessages.Count == 0)
                {
                    Task.Delay(10).Wait();
                    continue;
                }

                var match = _receivedServerMessages.FirstOrDefault();
                if (match != null)
                {
                    // Ta bort meddelandet från listan när det matchas
                    _receivedServerMessages.Remove(match);
                    return match;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"GetMessageFromClientQueueAsync failed: {ex.Message}");
            return null;
        }
        finally
        {
            _serverSemaphore.Release();
        }
    }

    private async Task ConsumeServerLoopAsync()
    {
        try
        {
            while (!_serverCts.Token.IsCancellationRequested)
            {
                var consumeResult = _serverConsumer.Consume(TimeSpan.FromSeconds(1));

                if (consumeResult?.Message == null)
                {
                    await Task.Delay(10, _serverCts.Token);
                    continue;
                }

                var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);

                lock (_serverLock)
                {
                    _receivedServerMessages.Add(entity);
                }

                _serverConsumer.Commit(consumeResult); // Bekräfta meddelandet
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

    private async Task<int> ProduceServerMessageAsync(QueueEntity entity)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.Leader,  // Snabbare leverans
            LingerMs = 0,        // Minska fördröjning innan meddelandet skickas
            BatchNumMessages = 1 // Skickar i batchar
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var jsonMessage = JsonSerializer.Serialize(entity);

        try
        {
            var result = await producer.ProduceAsync(_serverQueueTopic, new Message<Null, string> { Value = jsonMessage });
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
        _serverConsumer.Close();
        _serverConsumer.Dispose();
    }
}
