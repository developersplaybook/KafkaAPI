using Shared.Models;
using System.Threading.Tasks;
using System;

namespace Shared.Interfaces
{
    public interface IClientQueueRepository
    {
        Task<QueueEntity?> GetMessageFromServerByCorrelationIdAsync(Guid correlationId);
        Task<int> AddClientQueueItemAsync(QueueEntity entity);
    }
}
