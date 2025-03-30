using System;

namespace Shared.Requests
{
    [Serializable]
    public class GetCompaniesRequest
  {
        public GetCompaniesRequest()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
    }
}
