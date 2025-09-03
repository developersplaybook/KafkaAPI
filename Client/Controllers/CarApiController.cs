using Client.Interfaces;
using Client.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Shared.Models;
using Shared.Requests;
using Shared.Responses;
using Swashbuckle.AspNetCore.Annotations;
using System;
using System.Threading.Tasks;

namespace Client.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CarApiController : ControllerBase
    {
        private readonly SignInManager<ApplicationUser> _signInManager;
        private readonly IClientMessageHub _clientMessageHub;

        public CarApiController(SignInManager<ApplicationUser> signInManager, IClientMessageHub clientMessageHub)
        {
            _signInManager = signInManager;
            _clientMessageHub = clientMessageHub;
        }

        [HttpGet("getallcars")]
        [SwaggerOperation(Summary = "Get all cars", Description = "Get all cars")]
        [SwaggerResponse(200, "Respons was successfully generated")]
        [SwaggerResponse(500, "An unexpected error occurred")]
        public async Task<IActionResult> GetAllCars()
        {
            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarsRequest(), correlationId);
            var getCarsResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarsResponse>(correlationId);

            return Ok(getCarsResponse.Cars);
        }

        [HttpPost("updateonline")]
        [SwaggerOperation(Summary = "Update property online", Description = "Update property online")]
        [SwaggerResponse(200, "Update was successfull")]
        [SwaggerResponse(500, "An unexpected error occurred")]
        public async Task<IActionResult> UpdateOnline([FromBody] Car car)
        {
            if (!ModelState.IsValid) return Ok(new { success = false });

            var correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new GetCarRequest(car.Id), correlationId);
            var getCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<GetCarResponse>(correlationId);

            var oldCar = getCarResponse.Car;
            oldCar.Online = car.Online;

            correlationId = Guid.NewGuid();
            await _clientMessageHub.SendMessageToServerAsync(new UpdateCarRequest(oldCar), correlationId);
            var upadeCarResponse = await _clientMessageHub.ListenForMessageFromServerAsync<UpdateCarResponse>(correlationId);

            return Ok(new { success = true });
        }
    }
}