using Shared.Models;
using System;
using System.Collections.Generic;

namespace Shared.Responses
{
    [Serializable]
    public class GetCompaniesResponse
  {
        public GetCompaniesResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
        public List<Company> Companies { get; set; }
    }
}