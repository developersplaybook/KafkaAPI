using Shared.Models;
using Shared.DAL;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Threading;

namespace Shared.Repositories
{
  public class CompanyRepository : ICompanyRepository
  {
    private readonly CarApiDbContext _context;
    private readonly SemaphoreSlim _asyncLock = new(1, 1);
    public CompanyRepository(CarApiDbContext context)
    {
      _context = context;

    }

    public async Task<int> AddCompanyAsync(Company company)
    {
      await _asyncLock.WaitAsync();
      try
      {
        _context.Companies.Add(company);
        return await _context.SaveChangesAsync();
      }
      finally
      {
        _asyncLock.Release();
      }
    }

    public async Task<int> RemoveCompanyAsync(Guid Id)
    {
      await _asyncLock.WaitAsync();
      try
      {
        var foundCompany = await _context.Companies.Where(c => c.Id == Id).SingleOrDefaultAsync() ?? throw new Exception($"Company with id '{Id}' not found in database");
        _context.Companies.Remove(foundCompany);
        return await _context.SaveChangesAsync();
      }
      finally
      {
        _asyncLock.Release();
      }
    }
 

  public async Task<IEnumerable<Company>> GetAllCompaniesAsync()
  {
    return await _context.Companies.ToListAsync();
  }

  public async Task<Company> GetCompanyAsync(Guid Id)
  {
    return await _context.Companies.Where(c => c.Id == Id).SingleOrDefaultAsync();
  }

  public async Task<int> SaveContextChanges()
  {
    await _asyncLock.WaitAsync();
    try
    {
      return await _context.SaveChangesAsync();
    }
    finally
    {
      _asyncLock.Release();
    }
  }

  public async Task<int> UpdateCompanyAsync(Company company)
  {
    await _asyncLock.WaitAsync();
    try
    {

      var original = await GetCompanyAsync(company.Id) ?? throw new Exception($"Company with id '{company.Id}' not found in database");

      _context.Entry(original).CurrentValues.SetValues(company);

      try
      {
        // Save the changes to the database
        return await _context.SaveChangesAsync();
      }
      catch (DbUpdateConcurrencyException)
      {
        // Handle concurrency conflicts
        // You may want to retry the operation or implement custom logic here
        throw;
      }
    }
    finally
    {
      _asyncLock.Release();
    }
  }
}
}

