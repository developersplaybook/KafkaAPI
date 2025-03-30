using Client.Interfaces;
using Client.Models;
using Client.Models.HomeViewModel;
using Microsoft.AspNetCore.Mvc;
using Shared.Models;
using Shared.Requests;
using Shared.Responses;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Client.Controllers
{
    public class HomeController : Controller
    {
        readonly IClientMessageHub _serverMessageHub;
        private readonly JobStatusManager _jobStateManager;

        public HomeController(IClientMessageHub serverMessageHub, JobStatusManager jobStateManager)
        {
            _serverMessageHub = serverMessageHub;
            _jobStateManager = jobStateManager;
        }

        public async Task<IActionResult> Index()
        {
            List<Company> companies;
            try
            {
                var destinationAddr = Guid.NewGuid();
                await _serverMessageHub.SendMessageToServerAsync(new GetCompaniesRequest(), destinationAddr);
                var getCompaniesResponse = await _serverMessageHub.ListenForMessageFromServerAsync<GetCompaniesResponse>(destinationAddr);
                companies = getCompaniesResponse.Companies;
            }
            catch (Exception e)
            {
                TempData["CustomError"] = "Ingen kontakt med servern! CarAPI måste startas innan Client kan köras!";

                return View("Index", new HomeViewModel(Guid.NewGuid()) { Companies = new List<Company>() });
            }

            var correlationId = Guid.NewGuid();
            await _serverMessageHub.SendMessageToServerAsync(new GetCarsRequest(), correlationId);
            var getCarsResponse = await _serverMessageHub.ListenForMessageFromServerAsync<GetCarsResponse>(correlationId);

            var allCars = getCarsResponse.Cars.ToList();

            foreach (var car in allCars)
            {
                car.Disabled = false; //Enable updates of Online/Offline

                correlationId = Guid.NewGuid();
                await _serverMessageHub.SendMessageToServerAsync(new UpdateCarRequest(car), correlationId);
                var updateCarsResponse = await _serverMessageHub.ListenForMessageFromServerAsync<UpdateCarResponse>(correlationId);
            }

            foreach (var company in companies)
            {
                var companyCars = allCars.Where(o => o.CompanyId == company.Id).ToList();
                company.Cars = companyCars;
            }

            var homeViewModel = new HomeViewModel(Guid.NewGuid()) { Companies = companies, JobStateManager = _jobStateManager };

            return View("Index", homeViewModel);
        }

        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}