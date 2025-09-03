using Client.Interfaces;
using Client.Models;
using Client.Models.CarViewModel;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Shared.Models;
using Shared.Requests;
using Shared.Responses;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Client.Controllers
{
    [Route("[controller]")]
    [Controller]
    public class CarController : Controller
    {
        private readonly SignInManager<ApplicationUser> _signInManager;
        private readonly IClientMessageHub _clientMessageHub;

        public CarController(SignInManager<ApplicationUser> signInManager, IClientMessageHub clientMessageHub)
        {
            _signInManager = signInManager;
            _clientMessageHub = clientMessageHub;
        }

        [HttpGet("index")]
        public async Task<IActionResult> Index(Guid? id)
        {
            if (!_signInManager.IsSignedIn(User))
                return RedirectToAction("Index", "Home");

            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarsRequest(), correlationId);
            var getCarsResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarsResponse>(correlationId);

            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCompaniesRequest(), correlationId);
            var getCompaniesResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCompaniesResponse>(correlationId);

            var selectedCompany = id == null
                ? getCompaniesResponse.Companies.FirstOrDefault()
                : getCompaniesResponse.Companies.SingleOrDefault(c => c.Id == id);

            var companyId = selectedCompany?.Id ?? Guid.NewGuid();
            getCarsResponse.Cars = getCarsResponse.Cars.Where(c => c.CompanyId == companyId).ToList();

            var selectList = getCompaniesResponse.Companies.Select(c => new SelectListItem
            {
                Text = c.Name,
                Value = c.Id.ToString(),
                Selected = c.Id == companyId
            }).ToList();

            var viewModel = new CarListViewModel(companyId)
            {
                CompanySelectList = selectList,
                Cars = getCarsResponse.Cars
            };

            ViewBag.CompanyId = companyId;
            ViewBag.CompanyName = selectedCompany?.Name;

            return View(viewModel);
        }

        [HttpGet("details")]
        public async Task<IActionResult> Details(Guid id)
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarRequest(id), correlationId);
            var getCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarResponse>(correlationId);

            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCompanyRequest(getCarResponse.Car.CompanyId), correlationId);
            var getCompanyResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCompanyResponse>(correlationId);

            ViewBag.CompanyName = getCompanyResponse.Company.Name;
            return View(getCarResponse.Car);
        }

        [HttpGet("create")]
        public async Task<IActionResult> Create(string id)
        {
            var companyId = new Guid(id);
            var car = new Car(companyId);

            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCompanyRequest(companyId), correlationId);
            var getCompanyResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCompanyResponse>(correlationId);

            ViewBag.CompanyName = getCompanyResponse.Company.Name;
            return View(car);
        }

        // POST: Car/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost("create")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(
                [Bind("CompanyId,VIN,RegNr,Online")] Car car)
        {
            if (!ModelState.IsValid) return View(car);

            car.Id = Guid.NewGuid();
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new CreateCarRequest(car), correlationId);
            var createCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<CreateCarResponse>(correlationId);

            return RedirectToAction("Index", new { id = car.CompanyId });
        }

        [HttpGet("edit")]
        public async Task<IActionResult> Edit(Guid id)
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarRequest(id), correlationId);
            var getCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarResponse>(correlationId);

            getCarResponse.Car.Disabled = true; //Prevent updates of Online/Offline while editing
            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new UpdateCarRequest(getCarResponse.Car), correlationId);
            var updateCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<UpdateCarResponse>(correlationId);

            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCompanyRequest(getCarResponse.Car.CompanyId), correlationId);
            var getCompanyResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCompanyResponse>(correlationId);

            ViewBag.CompanyName = getCompanyResponse.Company.Name;

            return View(getCarResponse.Car);
        }

        // POST: Car/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost("edit")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(Guid id, [Bind("Id, Online")] Car car)
        {
            if (!ModelState.IsValid) return View(car);

            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarRequest(id), correlationId);
            var oldCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarResponse>(correlationId);

            var oldCar = oldCarResponse.Car;
            oldCar.Online = car.Online;
            oldCar.Disabled = false; //Enable updates of Online/Offline when editing done

            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new UpdateCarRequest(oldCar), correlationId);
            var updateCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<UpdateCarResponse>(correlationId);

            return RedirectToAction("Index", new { id = oldCar.CompanyId });
        }

        [HttpGet("delete")]
        public async Task<IActionResult> Delete(Guid id)
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarRequest(id), correlationId);
            var getCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarResponse>(correlationId);

            return View(getCarResponse.Car);
        }

        // POST: Car/Delete/5
        [HttpPost("delete")]
        [ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(Guid id)
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarRequest(id), correlationId);
            var getCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarResponse>(correlationId);

            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new DeleteCarRequest(id), correlationId);
            var deleteCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<DeleteCarResponse>(correlationId);

            return RedirectToAction("Index", new { id = getCarResponse.Car.CompanyId });
        }

        [HttpGet("regnravailableasync")]
        public async Task<JsonResult> RegNrAvailableAsync(string regNr)
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarsRequest(), correlationId);
            var getCarsResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarsResponse>(correlationId);

            bool isAvailable = getCarsResponse.Cars.All(c => c.RegNr != regNr);

            return Json(isAvailable);
        }

        [HttpGet("vinavailableasync")]
        public async Task<JsonResult> VinAvailableAsync(string vin)
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarsRequest(), correlationId);
            var getCarsResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarsResponse>(correlationId);

            bool isAvailable = getCarsResponse.Cars.All(c => c.VIN != vin);

            return Json(isAvailable);
        }
    }
}