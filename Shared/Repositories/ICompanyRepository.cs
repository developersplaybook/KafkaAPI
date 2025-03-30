using Shared.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Shared.Repositories
{
    public interface ICompanyRepository
    {
        public Task<int> AddCompanyAsync(Company car);

        public Task<int> UpdateCompanyAsync(Company car);

        public Task<Company> GetCompanyAsync(Guid Id);

        public Task<IEnumerable<Company>> GetAllCompaniesAsync();

        public Task<int> RemoveCompanyAsync(Guid Id);

        public Task<int> SaveContextChanges();
    }
}
