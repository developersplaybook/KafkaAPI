using System;

namespace Shared.Responses
{
    [Serializable]
    public class DeleteCarResponse
  {
        public DeleteCarResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
    }
}