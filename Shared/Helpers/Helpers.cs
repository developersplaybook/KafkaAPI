using Newtonsoft.Json;
using Shared.Requests;
using Shared.Responses;
using System;

namespace Shared.Helpers
{
    public class Helpers
    {
        public static object ConvertJsonToObject(string json, Type type)
        {
            return JsonConvert.DeserializeObject(json, type);
        }

        public static (string, Type) ConvertObjectToJson(object message)
        {
            var type = message.GetType();
            var json = JsonConvert.SerializeObject(message);
            return (json, type);
        }

        public static Type GetType(string className)
        {
            string[] classNameParts = className.Split('.');
            string simpleClassName = classNameParts[^1];
            return simpleClassName switch
            {
                nameof(CreateCarRequest) => typeof(CreateCarRequest),
                nameof(CreateCompanyRequest) => typeof(CreateCompanyRequest),
                nameof(DeleteCarRequest) => typeof(DeleteCarRequest),
                nameof(DeleteCompanyRequest) => typeof(DeleteCompanyRequest),
                nameof(GetCarRequest) => typeof(GetCarRequest),
                nameof(GetCarsRequest) => typeof(GetCarsRequest),
                nameof(GetCompanyRequest) => typeof(GetCompanyRequest),
                nameof(GetCompaniesRequest) => typeof(GetCompaniesRequest),
                nameof(UpdateCarRequest) => typeof(UpdateCarRequest),
                nameof(UpdateCompanyRequest) => typeof(UpdateCompanyRequest),
                nameof(CreateCarResponse) => typeof(CreateCarResponse),
                nameof(CreateCompanyResponse) => typeof(CreateCompanyResponse),
                nameof(DeleteCarResponse) => typeof(DeleteCarResponse),
                nameof(DeleteCompanyResponse) => typeof(DeleteCompanyResponse),
                nameof(GetCarResponse) => typeof(GetCarResponse),
                nameof(GetCarsResponse) => typeof(GetCarsResponse),
                nameof(GetCompanyResponse) => typeof(GetCompanyResponse),
                nameof(GetCompaniesResponse) => typeof(GetCompaniesResponse),
                nameof(UpdateCarResponse) => typeof(UpdateCarResponse),
                nameof(UpdateCompanyResponse) => typeof(UpdateCompanyResponse),
                _ => throw new NotSupportedException($"Request type {className} is not supported."),
            };
        }
    }
}
