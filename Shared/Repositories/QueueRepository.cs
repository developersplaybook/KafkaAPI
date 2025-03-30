using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Shared.Models;

namespace Shared.Repositories;
public class QueueRepository : IQueueRepository
{
    private readonly string _bootstrapServers = "localhost:9092";
    private readonly string _clientQueueTopic = "client-queue";
    private readonly string _serverQueueTopic = "server-queue";

    public async Task<int> AddClientQueueItemAsync(QueueEntity entity)
    {
        return await ProduceMessageAsync(_clientQueueTopic, entity);
    }

    public async Task<int> AddServerQueueItemAsync(QueueEntity entity)
    {
        return await ProduceMessageAsync(_serverQueueTopic, entity);
    }

    public async Task<QueueEntity?> GetMessageFromClientQueueAsync()
    {
        return await ConsumeMessageAsync(_clientQueueTopic);
    }

    public async Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId)
    {
        return await ConsumeMessageAsync(_serverQueueTopic, correlationId);
    }

    private async Task<int> ProduceMessageAsync(string topic, QueueEntity entity)
    {
        var config = new ProducerConfig { BootstrapServers = _bootstrapServers };
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

    private async Task<QueueEntity?> ConsumeMessageAsync(string topic, Guid? correlationId = null)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "queue-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(topic);

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();
                var entity = JsonSerializer.Deserialize<QueueEntity>(consumeResult.Message.Value);

                if (correlationId == null || entity?.CorrelationId == correlationId)
                {
                    consumer.Commit(consumeResult);
                    return entity;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consuming message failed: {ex.Message}");
            return null;
        }
        finally
        {
            consumer.Close();
        }
    }
}
