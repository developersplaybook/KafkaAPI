using System;

namespace Shared.Responses
{
    [Serializable]
    public class DeleteCompanyResponse
  {
        public DeleteCompanyResponse()
        {
            DataId = Guid.NewGuid();
        }

        public Guid DataId { get; set; }
    }
}