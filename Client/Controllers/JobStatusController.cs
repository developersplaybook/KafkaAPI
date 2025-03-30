using Client.Models;
using Microsoft.AspNetCore.Mvc;
using System;

[Route("api/[controller]")]
[ApiController]
public class JobStatusController : ControllerBase
{
    private readonly JobStatusManager _jobStateManager;

    public JobStatusController(JobStatusManager jobStateManager)
    {
        _jobStateManager = jobStateManager;
    }

    [HttpPost("update")]
    public IActionResult UpdateJobState([FromBody] JobStatusUpdateModel updateModel)
    {
        try
        {
            _jobStateManager.UpdateJobStatus(updateModel.JobName, updateModel.Status);
            return Ok($"Job {updateModel.JobName}'s state updated to {updateModel.Status}.");
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }
    }

    [HttpGet("toggle")]
    public IActionResult ToggleJobStaus([FromQuery] JobName jobName)
    {
        try
        {
            _jobStateManager.ToggleJobStatus(jobName);
            return Ok($"Job {jobName}'s status toggled.");
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }
    }

    [HttpGet("getalljobs")]
    public IActionResult GetJobs()
    {
        try
        {
            return Ok(_jobStateManager.GetAllJobs());
        }
        catch (ArgumentException ex)
        {
            return BadRequest(ex.Message);
        }
    }
}

