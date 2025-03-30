using System;

namespace Shared.Requests
{
    [Serializable]
    public class GetCarsRequest
  {
        public GetCarsRequest()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
    }
}
