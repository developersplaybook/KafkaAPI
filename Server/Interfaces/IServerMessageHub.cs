using Shared.Models;
using System;
using System.Threading.Tasks;

namespace Server.Interfaces
{
    public interface IServerMessageHub
    {
        Task ListenForClientMessageAsync();
        Task HandleMessageFromClientAsync(QueueEntity queuePackage);
        Task SendMessageToClientAsync(object message, Guid correlationId);
    }
}
