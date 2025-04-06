using Shared.Models;
using System.Threading.Tasks;

namespace Shared.Interfaces
{
    public interface IServerQueueRepository
    {
        Task<QueueEntity?> GetMessageFromClientQueueAsync();
        Task<int> AddServerQueueItemAsync(QueueEntity entity);
    }
}
