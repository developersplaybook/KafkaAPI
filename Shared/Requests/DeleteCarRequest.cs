using System;

namespace Shared.Requests
{
    [Serializable]
    public class DeleteCarRequest
  {
        public DeleteCarRequest(Guid carId)
        {
            DataId = Guid.NewGuid();
            CarId = carId;
        }

        public Guid DataId { get; set; }
        public Guid CarId { get; set; }
    }
}