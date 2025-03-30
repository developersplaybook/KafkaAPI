using Microsoft.EntityFrameworkCore;
using Shared.Models;
using System.IO;
using System;

namespace Shared.DAL
{
    public class CarApiDbContext : DbContext
    {
        public CarApiDbContext(DbContextOptions options)
            : base(options)
        {
        }
        public DbSet<Car> Cars { get; set; }

        public DbSet<Company> Companies { get; set; }
    }
}