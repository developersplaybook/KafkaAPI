using Newtonsoft.Json;
using Server.Interfaces;
using Shared.Interfaces;
using Shared.Helpers;
using Shared.Models;
using Shared.Requests;
using Shared.Responses;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Server.Hub
{
    public class ServerMessageHub : IServerMessageHub
    {
        private readonly IServerQueueRepository _queueRepository;
        private readonly ICompanyRepository _companyRepository;
        private readonly ICarRepository _carRepository;

        public ServerMessageHub(IServerQueueRepository queueRepository, ICompanyRepository companyRepository, ICarRepository carRepository)
        {
            _queueRepository = queueRepository;
            _companyRepository = companyRepository;
            _carRepository = carRepository;
        }

        public async Task ListenForClientMessageAsync()
        {
            while (true)
            {
                var clientMessage = await _queueRepository.GetMessageFromClientQueueAsync(); ;
                if (clientMessage == null) break;

                await HandleMessageFromClientAsync(clientMessage);
            }
        }

        public async Task HandleMessageFromClientAsync(QueueEntity clientMessage)
        {
            string[] classNameParts = clientMessage.TypeName.Split('.');
            string simpleClassName = classNameParts[^1];
            var requestMessage = JsonConvert.DeserializeObject(clientMessage.Content, Helpers.GetType(clientMessage.TypeName));

            Task<object> result = simpleClassName switch
            {
                nameof(CreateCarRequest) => HandleCreateCarRequest((CreateCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(CreateCompanyRequest) => HandleCreateCompanyRequest((CreateCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(DeleteCarRequest) => HandleDeleteCarRequest((DeleteCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(DeleteCompanyRequest) => HandleDeleteCompanyRequest((DeleteCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCarRequest) => HandleGetCarRequest((GetCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCarsRequest) => HandleGetCarsRequest((GetCarsRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCompanyRequest) => HandleGetCompanyRequest((GetCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(GetCompaniesRequest) => HandleGetCompaniesRequest((GetCompaniesRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(UpdateCarRequest) => HandleUpdateCarRequest((UpdateCarRequest)requestMessage).ContinueWith(task => (object)task.Result),
                nameof(UpdateCompanyRequest) => HandleUpdateCompanyRequest((UpdateCompanyRequest)requestMessage).ContinueWith(task => (object)task.Result),
                _ => throw new NotSupportedException($"Request type {clientMessage.TypeName} is not supported.")
            };

            object actualResult = await result;
            await SendMessageToClientAsync(actualResult, clientMessage.CorrelationId);
        }

        public async Task SendMessageToClientAsync(object message, Guid correlationId)
        {
            var result = Helpers.ConvertObjectToJson(message);
            var entity = new QueueEntity
            {
                CorrelationId = correlationId,
                Content = result.Item1,
                TypeName = result.Item2.ToString(),
                Created = DateTime.Now,
                StatusDate = DateTime.Now,
            };

            await _queueRepository.AddServerQueueItemAsync(entity);

        }

        // Message Handlers
        private async Task<CreateCarResponse> HandleCreateCarRequest(CreateCarRequest request)
        {
            await _carRepository.AddCarAsync(request.Car);
            return new CreateCarResponse { DataId = request.DataId, Car = request.Car };
        }

        private async Task<CreateCompanyResponse> HandleCreateCompanyRequest(CreateCompanyRequest request)
        {
            await _companyRepository.AddCompanyAsync(request.Company);
            return new CreateCompanyResponse { DataId = request.DataId, Company = request.Company };
        }

        private async Task<DeleteCarResponse> HandleDeleteCarRequest(DeleteCarRequest request)
        {
            await _carRepository.RemoveCarAsync(request.CarId);
            return new DeleteCarResponse { DataId = request.DataId };
        }

        private async Task<DeleteCompanyResponse> HandleDeleteCompanyRequest(DeleteCompanyRequest request)
        {
            await _companyRepository.RemoveCompanyAsync(request.CompanyId);
            return new DeleteCompanyResponse { DataId = request.DataId };
        }

        private async Task<GetCarResponse> HandleGetCarRequest(GetCarRequest request)
        {
            var car = await _carRepository.GetCarAsync(request.CarId);
            return new GetCarResponse { DataId = request.DataId, Car = car };
        }

        private async Task<GetCarsResponse> HandleGetCarsRequest(GetCarsRequest request)
        {
            var cars = await _carRepository.GetAllCarsAsync();
            return new GetCarsResponse { DataId = request.DataId, Cars = cars.ToList() };
        }

        private async Task<GetCompanyResponse> HandleGetCompanyRequest(GetCompanyRequest request)
        {
            var company = await _companyRepository.GetCompanyAsync(request.CompanyId);
            return new GetCompanyResponse { DataId = request.DataId, Company = company };
        }

        private async Task<GetCompaniesResponse> HandleGetCompaniesRequest(GetCompaniesRequest request)
        {
            var companies = await _companyRepository.GetAllCompaniesAsync();
            return new GetCompaniesResponse { DataId = request.DataId, Companies = companies.ToList() };
        }

        private async Task<UpdateCarResponse> HandleUpdateCarRequest(UpdateCarRequest request)
        {
            await _carRepository.UpdateCarAsync(request.Car);
            return new UpdateCarResponse { DataId = request.DataId, Car = request.Car };
        }

        private async Task<UpdateCompanyResponse> HandleUpdateCompanyRequest(UpdateCompanyRequest request)
        {
            await _companyRepository.UpdateCompanyAsync(request.Company);
            return new UpdateCompanyResponse { DataId = request.DataId, Company = request.Company };
        }
    }
}
