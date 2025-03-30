using System;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Shared.Models
{
  [Serializable]
  public class Car : IEquatable<Car>
  {
		public Car()
		{
			Id = Guid.NewGuid();
			CreationTime = DateTime.Now.ToString(new CultureInfo("se-SE"));
			Online = true;
		}

		public Car(Guid companyId):this()
		{
			CompanyId = companyId;
		}

		[Key]
		public Guid Id { get; set; }
		public Guid CompanyId { get; set; }

		[Display(Name = "Created date")]
		public string CreationTime { get; set; }

		[Display(Name = "VIN (VehicleID)")]
		[RegularExpression(@"^[A-Z0-9]{6}\d{11}$", ErrorMessage = "{0} denoted as X1Y2Z300001239876")]
		[Remote("VinAvailableAsync", "Car", ErrorMessage = "VIN already taken")]
		public string VIN { get; set; }

		[Display(Name = "Reg. Nbr.")]
		[RegularExpression(@"^[A-Z]{3}\d{3}$", ErrorMessage = "{0} denoted as XYZ123")]
		[Remote("RegNrAvailableAsync", "Car", ErrorMessage = "Registration number taken")]
		public string RegNr { get; set; }

		[Display(Name = "Status")]
		public bool Online { get; set; }

		[Display(Name = "Online (X) or Offline ()?")]
		public string OnlineOrOffline => (this.Online) ? "Online" : "Offline";

		public bool Disabled { get; set; } //Used to block changes of Online/Offline status

    public override bool Equals(object obj)
    {
      if (obj == null || !(obj is Car))
      {
        return false;
      }

      return Equals((Car)obj);
    }

    public bool Equals(Car other)
    {
      if (other == null)
      {
        return false;
      }

      return GetHashCode() == other.GetHashCode();
    }

    public override int GetHashCode()
    {
      return HashCode.Combine(CompanyId, Id, CreationTime, Disabled, VIN, RegNr, Online);
    }

		public Car Copy()
    {
      return new Car
      {

        CompanyId = CompanyId,
        Id = Id,
        CreationTime = CreationTime,
        Disabled = Disabled,
        VIN = VIN,
        RegNr = RegNr,
        Online = Online
      };
    }
  }
}
