using System;

namespace Shared.Requests
{
    [Serializable]
    public class GetCompanyRequest
  {
        public GetCompanyRequest(Guid companyId)
        {
            DataId = Guid.NewGuid();
            CompanyId = companyId;
        }

        public Guid DataId { get; set; }
        public Guid CompanyId { get; set; }
    }
}
