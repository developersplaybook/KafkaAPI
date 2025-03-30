using Shared.Models;
using System;

namespace Shared.Responses
{

    [Serializable]
    public class UpdateCompanyResponse
  {
        public UpdateCompanyResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
        public Company Company { get; set; }
    }
}