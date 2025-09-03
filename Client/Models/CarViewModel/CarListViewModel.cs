using Microsoft.AspNetCore.Mvc.Rendering;
using System;
using System.Collections.Generic;

namespace Client.Models.CarViewModel
{
    using Shared.Models;

    public class CarListViewModel : Car
    {
        public CarListViewModel(Guid companyId) : base(companyId)
        {
            CompanyId = companyId;
        }

        private new Guid CompanyId { get; set; }
        public List<SelectListItem> CompanySelectList { get; set; }
        public List<Car> Cars { get; set; }
    }
}