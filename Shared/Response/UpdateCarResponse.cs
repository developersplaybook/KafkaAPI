using Shared.Models;
using System;

namespace Shared.Responses
{
    [Serializable]
    public class UpdateCarResponse
  {
        public UpdateCarResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
        public Car Car { get; set; }
    }
}