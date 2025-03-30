using Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Shared.Repositories
{
	public interface ICarRepository
    {
    public Task<int> AddCarAsync(Car car);

    public Task<int> UpdateCarAsync(Car car);
    
    public Task<Car?> GetCarAsync(Guid Id);
    
    public Task<IEnumerable<Car>> GetAllCarsAsync();

    public Task<int> RemoveCarAsync(Guid Id);

    public Task<int> SaveContextChanges();
  }
}
