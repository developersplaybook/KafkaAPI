using Shared.Models;
using System;

namespace Shared.Responses
{
    [Serializable]
    public class GetCompanyResponse
  {
        public GetCompanyResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
        public Company Company { get; set; }
    }
}