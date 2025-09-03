$(document).ready(() => {
  const apiPort = 63566;

  const ShowCars = () => {
    window.location = "/Car/index?id=" + $('#CarsForCompany').val();
  };

  // Event binding for the dropdown change event
  $('#CarsForCompany').on('change', ShowCars);

  document.querySelectorAll('[name="status"]').forEach(button => {
    button.addEventListener('click', doFiltering);
  });

  carTimerJob(apiPort);
  jobTimer(apiPort);
  console.log('documentReady');
});

$(".uppercase").on('keyup', function () {
  let text = $(this).val();
  $(this).val(text.toUpperCase());
});

const clearErrors = () => {
  $(".validation-summary-errors").empty();
  var errorElements = document.getElementsByClassName('text-danger');
  for (var i = 0; i < errorElements.length; i++) {
    errorElements[i].innerHTML = '';
  }
};

const setupClearErrors = () => {
  clearErrors();
  var form = document.getElementById('CreateForm');
  if (form) {
    form.addEventListener('mousedown', clearErrors);
  }
}

document.addEventListener('DOMContentLoaded', setupClearErrors);

const carTimerJob = (apiPort) => {
  const halfSecond = 500;
  const oneTenthSecond = 100;
  $.ajax({
    url: `http://localhost:${apiPort}/api/carapi/getallcars`,
    type: "GET",
    dataType: "json",
    success: (cars) => {
      if (cars.length === 0) {
        setTimeout(() => carTimerJob(apiPort), oneTenthSecond);
        console.log("No cars were found!");
        return;
      }
      const selectedItem = Math.floor(Math.random() * cars.length);
      let selectedCar = cars[selectedItem];
      if (selectedCar.disabled === true) {
        console.log(selectedCar.regNr + " is blocked for updating of Online/Offline!");
        return;
      }
      selectedCar.online = !selectedCar.online;
      $.ajax({
        url: `http://localhost:${apiPort}/api/carapi/updateonline`,
        contentType: "application/json",
        type: "POST",
        data: JSON.stringify(selectedCar),
        dataType: "json",
        success: (response) => {
        },
        error: (error) => {
        }
      });

      const selector = `#${selectedCar.id} td:eq(2)`;
      const selector2 = `#${selectedCar.id + "_2"} td:eq(3)`;
      const selector3 = `#${selectedCar.id + "_3"}`;
      if (selectedCar.online === true) {
        $(selector).text("Online");
        $(selector).removeClass("alert-danger");
        $(selector2).text("Online");
        $(selector2).removeClass("alert-danger");
        $(selector3).text("Online");
        $(selector3).removeClass("alert-danger");
        console.log(selectedCar.regNr + " is Online!");
      } else {
        $(selector).text("Offline");
        $(selector).addClass("alert-danger");
        $(selector2).text("Offline");
        $(selector2).addClass("alert-danger");
        $(selector3).text("Offline");
        $(selector3).addClass("alert-danger");
        console.log(selectedCar.regNr + " is Offline!");
      }
      if (document.getElementById("All") !== null) {
        doFiltering();
      }
    }
  });
  setTimeout(() => carTimerJob(apiPort), halfSecond);
};

const doFiltering = () => {
  let selection = 0;
  let radiobtn = document.getElementById("All");
  if (radiobtn.checked === false) {
    radiobtn = document.getElementById("Online");
    if (radiobtn.checked === true) {
      selection = 1;
    } else {
      selection = 2;
    }
  }

  var table = $('#cars > tbody');
  $('tr', table).each(function () {
    $(this).removeClass("hidden");
    let td = $('td:eq(2)', $(this)).html();
    if (td !== undefined) {
      td = td.trim();
    }
    if (td === "Offline" && selection === 1) {
      $(this).addClass("hidden");  // Show only Online
    }
    if (td === "Online" && selection === 2) {
      $(this).addClass("hidden"); // Show only Offline
    }
  });
};

let oldJobs = {};

const jobTimer = (apiPort) => {
  const oneTenthSecond = 100;

  $.ajax({
    url: `http://localhost:${apiPort}/api/jobstatus/getalljobs`,
    type: "GET",
    dataType: "json",
    success: (jobs) => {
      // Check if jobs object is empty
      if (Object.keys(jobs).length === 0) {
        console.log("No jobs found!");
      } else {
        Object.entries(jobs).forEach(([key, job]) => {
          if (!oldJobs[key] || oldJobs[key].status !== job.status) {
            setJobStatus(key, job.status);
          }
        });
      }

      oldJobs = jobs;
    },
    error: (jqXHR, textStatus, errorThrown) => {
      console.error("AJAX call failed: ", textStatus, errorThrown);
    }
  });

  setTimeout(() => jobTimer(apiPort), oneTenthSecond); // Schedule the next call
};

function setJobStatus(jobId, newStatus) {
  const statusContainer = document.getElementById('job_' + jobId);
  if (!statusContainer) return;

  statusContainer.innerHTML = '';

  // Based on the new status, update the content
  if (newStatus === 'Running') {
    console.log("setJobStatus Spinner: ", jobId, newStatus)
    const spinnerHTML = `
            <svg viewBox="0 0 800 800" xmlns="http://www.w3.org/2000/svg" width="60" height="60">
                <style>
                    @keyframes spin {
                        to {
                            transform: rotate(360deg);
                        }
                    }

                    @keyframes spin2 {
                        0% {
                            stroke-dasharray: 1, 800;
                            stroke-dashoffset: 0;
                        }
                        50% {
                            stroke-dasharray: 400, 400;
                            stroke-dashoffset: -200px;
                        }
                        100% {
                            stroke-dasharray: 800, 1;
                            stroke-dashoffset: -800px;
                        }
                    }

                    .spin2 {
                        transform-origin: center;
                        animation: spin2 1.5s ease-in-out infinite,
                        spin 2s linear infinite;
                        animation-direction: alternate;
                    }
                </style>
                <circle class="spin2" cx="400" cy="400" fill="none" r="200" stroke-width="50" stroke="#E387FF" stroke-dasharray="700 1400" stroke-linecap="round"></circle>
            </svg>
        `;
    statusContainer.innerHTML = spinnerHTML;
  } else {
    // If status is not Running, show the status text
    const statusSpan = document.createElement('span');
    statusSpan.className = 'text-state';
    statusSpan.textContent = newStatus; // Assuming newStatus is 'Paused' or another textual status
    statusContainer.appendChild(statusSpan);
  }
}