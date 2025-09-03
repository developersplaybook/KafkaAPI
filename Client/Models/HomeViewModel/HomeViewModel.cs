using Shared.Models;
using System;
using System.Collections.Generic;

namespace Client.Models.HomeViewModel
{
    public class HomeViewModel : Car
    {
        public HomeViewModel(Guid companyId) : base(companyId)
        {
            CompanyId = companyId;
        }

        public List<Company> Companies { get; set; }

        public JobStatusManager JobStateManager { get; set; }
    }
}