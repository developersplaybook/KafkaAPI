using Shared.Models;
using System;

namespace Shared.Requests
{

    [Serializable]
    public class CreateCompanyRequest
  {
        public CreateCompanyRequest(Company company)
        {
            DataId = Guid.NewGuid();
            Company = company;
        }

        public Guid DataId { get; set; }
        public Company Company { get; set; }
    }
}