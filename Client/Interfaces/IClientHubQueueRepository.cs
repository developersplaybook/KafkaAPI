using Shared.Models;
using System.Threading.Tasks;
using System;

namespace Client.Interfaces
{
    public interface IClientHubQueueRepository
    {
        Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId);
        Task<int> AddClientQueueItemAsync(QueueEntity entity);
    }
}
