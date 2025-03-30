using System.Text.Json.Serialization;

namespace Client.Models
{
    public class JobInfo
    {
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public JobStatus Status { get; set; }
        public string ImageName { get; set; }

        public JobInfo(JobStatus status, string imageName)
        {
            Status = status;
            ImageName = imageName;
        }
    }

}
