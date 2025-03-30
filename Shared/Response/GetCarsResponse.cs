using Shared.Models;
using System;
using System.Collections.Generic;

namespace Shared.Responses
{
    [Serializable]
    public class GetCarsResponse
  {
        public GetCarsResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
        public List<Car> Cars { get; set; }
    }
}