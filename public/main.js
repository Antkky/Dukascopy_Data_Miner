document.addEventListener("DOMContentLoaded", function () {
  // Initial data load
  loadAllData();

  // Setup refresh button
  document.getElementById("refreshBtn").addEventListener("click", function () {
    loadAllData();
    this.classList.add("rotate-animation");
    setTimeout(() => {
      this.classList.remove("rotate-animation");
    }, 1000);
  });

  // Setup symbol search
  document
    .getElementById("symbolSearch")
    .addEventListener("input", function () {
      filterSymbols(this.value.toLowerCase());
    });

  // Setup tab click event to reload data for the active tab
  document.querySelectorAll("#dataTabs button").forEach((tab) => {
    tab.addEventListener("click", function () {
      const tabId = this.getAttribute("data-bs-target").substring(1);
      switch (tabId) {
        case "symbols":
          loadSymbolData();
          break;
        case "logs":
          loadLogData();
          break;
        case "checkpoint":
          loadCheckpointData();
          break;
      }
    });
  });
});

function loadAllData() {
  loadOverallProgress();
  loadSymbolData();
  loadLogData();
  loadCheckpointData();
  updateLastUpdated();
}

function updateLastUpdated() {
  const now = new Date();
  document.getElementById(
    "lastUpdated"
  ).textContent = `Last updated: ${now.toLocaleTimeString()}`;
}

function loadOverallProgress() {
  fetch("/api/progress")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Failed to fetch overall progress");
      }
      return response.json();
    })
    .then((data) => {
      const overallProgressEl = document.getElementById("overallProgress");

      const stats = data.overall_stats;
      const dateFrom = stats.date_range.from
        ? new Date(stats.date_range.from).toLocaleDateString()
        : "N/A";
      const dateTo = stats.date_range.to
        ? new Date(stats.date_range.to).toLocaleDateString()
        : "N/A";

      overallProgressEl.innerHTML = `
        <div class="row">
          <div class="col-md-8">
            <h4 class="mb-3">Completion: ${stats.completion_percentage}%</h4>
            <div class="progress mb-4">
              <div class="progress-bar bg-success" role="progressbar" style="width: ${
                stats.completion_percentage
              }%"
                aria-valuenow="${
                  stats.completion_percentage
                }" aria-valuemin="0" aria-valuemax="100">
                ${stats.completion_percentage}%
              </div>
            </div>
          </div>
          <div class="col-md-4">
            <div class="card bg-light">
              <div class="card-body">
                <p class="mb-1"><strong>Date Range:</strong> ${dateFrom} - ${dateTo}</p>
                <p class="mb-1"><strong>Total Symbols:</strong> ${
                  stats.total_symbols
                }</p>
                <p class="mb-1"><strong>Tables with Data:</strong> ${
                  stats.tables_with_data
                }</p>
                <p class="mb-0"><strong>Total Records:</strong> ${stats.total_records.toLocaleString()}</p>
              </div>
            </div>
          </div>
        </div>
      `;
    })
    .catch((error) => {
      console.error("Error loading overall progress:", error);
      document.getElementById("overallProgress").innerHTML = `
        <div class="alert alert-danger">
          Failed to load progress data: ${error.message}
        </div>
      `;
    });
}

function loadSymbolData() {
  fetch("/api/progress")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Failed to fetch symbol data");
      }
      return response.json();
    })
    .then((data) => {
      const symbolGrid = document.getElementById("symbolGrid");
      const symbolStats = data.symbol_stats;

      // Sort by symbol name
      symbolStats.sort((a, b) => a.symbol.localeCompare(b.symbol));

      let html = "";

      symbolStats.forEach((stat) => {
        const cardClass = stat.has_data ? "border-success" : "border-danger";
        const iconClass = stat.has_data
          ? "bi-check-circle-fill text-success"
          : "bi-x-circle-fill text-danger";
        const dateRangeText = stat.has_data
          ? `${new Date(stat.oldest_date).toLocaleDateString()} - ${new Date(
              stat.newest_date
            ).toLocaleDateString()}`
          : "No data";

        html += `
          <div class="card symbol-card ${cardClass}" data-symbol="${
          stat.symbol
        }">
            <div class="card-header d-flex justify-content-between align-items-center">
              <span class="fw-bold">${stat.symbol.toUpperCase()}</span>
              <i class="bi ${iconClass}"></i>
            </div>
            <div class="card-body">
              <p class="mb-1"><strong>Records:</strong> ${stat.total_records.toLocaleString()}</p>
              <p class="mb-0"><small><strong>Date Range:</strong><br>${dateRangeText}</small></p>
            </div>
          </div>
        `;
      });

      symbolGrid.innerHTML = html;

      // Apply current filter if search box has value
      const searchTerm = document
        .getElementById("symbolSearch")
        .value.toLowerCase();
      if (searchTerm) {
        filterSymbols(searchTerm);
      }
    })
    .catch((error) => {
      console.error("Error loading symbol data:", error);
      document.getElementById("symbolGrid").innerHTML = `
        <div class="alert alert-danger">
          Failed to load symbol data: ${error.message}
        </div>
      `;
    });
}

function filterSymbols(searchTerm) {
  const symbolCards = document.querySelectorAll(".symbol-card");

  symbolCards.forEach((card) => {
    const symbol = card.getAttribute("data-symbol");
    if (symbol.includes(searchTerm)) {
      card.style.display = "";
    } else {
      card.style.display = "none";
    }
  });
}

function loadLogData() {
  fetch("/api/logs")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Failed to fetch logs");
      }
      return response.json();
    })
    .then((data) => {
      const logContainer = document.getElementById("logContainer");

      if (!data.log_file) {
        logContainer.innerHTML =
          '<div class="alert alert-info">No log files found.</div>';
        return;
      }

      let html = `
        <h5>Log File: ${data.log_file}</h5>
        <p>Total lines: ${data.total_lines.toLocaleString()}</p>
        <p>Showing latest ${data.recent_lines.length} lines:</p>
      `;

      data.recent_lines.forEach((line) => {
        const isError = line.includes("[ERROR]");
        const lineClass = isError ? "log-error" : "";
        html += `<pre class="log-line ${lineClass}">${line}</pre>`;
      });

      logContainer.innerHTML = html;

      // Auto-scroll to bottom of log container
      logContainer.scrollTop = logContainer.scrollHeight;
    })
    .catch((error) => {
      console.error("Error loading logs:", error);
      document.getElementById("logContainer").innerHTML = `
        <div class="alert alert-danger">
          Failed to load logs: ${error.message}
        </div>
      `;
    });
}

function loadCheckpointData() {
  fetch("/api/checkpoint")
    .then((response) => {
      if (!response.ok) {
        throw new Error("Failed to fetch checkpoint data");
      }
      return response.json();
    })
    .then((data) => {
      const checkpointCard = document.getElementById("checkpointCard");

      const date = new Date(data.date).toLocaleString();

      checkpointCard.innerHTML = `
        <div class="card-header bg-info text-white">
          <h5 class="mb-0">Current Checkpoint</h5>
        </div>
        <div class="card-body">
          <div class="row">
            <div class="col-md-6">
              <p><strong>Date:</strong> ${date}</p>
              <p><strong>Last Symbol:</strong> ${data.lastSymbol.toUpperCase()}</p>
            </div>
            <div class="col-md-6">
              <div class="alert alert-info">
                <p class="mb-0">The import script will resume from this symbol on the next date.</p>
              </div>
            </div>
          </div>
        </div>
      `;
    })
    .catch((error) => {
      console.error("Error loading checkpoint data:", error);
      document.getElementById("checkpointCard").innerHTML = `
        <div class="card-body">
          <div class="alert alert-danger">
            Failed to load checkpoint data: ${error.message}
          </div>
        </div>
      `;
    });
}
