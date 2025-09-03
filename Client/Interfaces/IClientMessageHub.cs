using System;
using System.Threading.Tasks;

namespace Client.Interfaces
{
    public interface IClientMessageHub
    {
        Task SendMessageToServerAsync(object message, Guid correlationId);

        Task<TResponse> ListenForMessageFromServerAsync<TResponse>(Guid correlationId);
    }
}