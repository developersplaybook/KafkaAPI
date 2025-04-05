using Shared.Models;
using System.Threading.Tasks;
using System;

namespace Server.Interfaces
{
    public interface IServerHubQueueRepository
    {
        Task<QueueEntity?> GetMessageFromClientQueueAsync();
        Task<int> AddServerQueueItemAsync(QueueEntity entity);
    }
}
