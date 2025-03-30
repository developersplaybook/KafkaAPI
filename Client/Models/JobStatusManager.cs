using System.Collections.Generic;
using System.Linq;

namespace Client.Models
{
    public enum JobStatus
    {
        Paused,
        Running
    }

    public enum JobName
    {
        AirJob,
        BoatJob,
        SunJob,
        TruckJob
    }
    public class JobStatusManager
    {
        private Dictionary<JobName, JobInfo> jobStatus;

        public JobStatusManager()
        {
            jobStatus = new Dictionary<JobName, JobInfo>
        {
            {JobName.AirJob, new JobInfo(JobStatus.Paused, "airplane.svg")},
            {JobName.BoatJob, new JobInfo(JobStatus.Paused, "sailingBoat.svg")},
            {JobName.SunJob, new JobInfo(JobStatus.Paused, "sunChair.svg")},
            {JobName.TruckJob, new JobInfo(JobStatus.Paused, "truck.svg")}
        };
        }

        // Methods to interact with jobStates
        public void UpdateJobStatus(JobName jobName, JobStatus status)
        {
            if (jobStatus.ContainsKey(jobName))
            {
                jobStatus[jobName].Status = status;
            }
            else
            {
                jobStatus.Add(jobName, new JobInfo(status, "image"));
            }
        }

        public void ToggleJobStatus(JobName jobName)
        {
            if (jobStatus.ContainsKey(jobName))
            {
                if (jobStatus[jobName].Status == JobStatus.Paused)
                {
                    jobStatus[jobName].Status = JobStatus.Running;
                }
                else
                {
                    jobStatus[jobName].Status = JobStatus.Paused;
                }
            }
            else
            {
                jobStatus.Add(jobName, new JobInfo(JobStatus.Paused, "image"));
            }
        }

        public JobInfo GetJobInfo(JobName jobName)
        {
            if (jobStatus.TryGetValue(jobName, out JobInfo jobInfo))
            {
                return jobInfo;
            }
            return null; // Or throw an exception or handle accordingly
        }

        public Dictionary<JobName, JobInfo> GetAllJobs()
        {
            return jobStatus;
        }
    }
}
