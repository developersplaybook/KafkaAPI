using System;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Text.RegularExpressions;

namespace Shared.Models
{
  [Serializable]
  public class Company : IEquatable<Company>
  {
		public Company()
		{
			CreationTime = DateTime.Now.ToString(new CultureInfo("se-SE"));
		}
		public Company(Guid id):this()
		{
			Id = id;
		}

    [Key]
    public Guid Id { get; set; }

		[Display(Name = "Created date")]
		public string CreationTime { get; set; }
		[Display(Name = "Name")]
		public string Name { get; set; }

		[Display(Name = "Address")]
		public string Address { get; set; }

		public ICollection<Car> Cars { get; set; }

    public override bool Equals(object obj)
    {
      if (obj == null || !(obj is Company))
      {
        return false;
      }

      return Equals((Company)obj);
    }

    public bool Equals(Company other)
    {
      if (other == null)
      {
        return false;
      }

      return GetHashCode() == other.GetHashCode();
    }

    public override int GetHashCode()
    {
      return HashCode.Combine(Id, CreationTime, Name, Address);
    }
  }
}
