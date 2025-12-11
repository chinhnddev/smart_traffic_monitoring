// Global variables
let crowdChart;
let locationChart;
let dataTable;
let currentFilters = {
    location_id: 'all',
    start_date: '',
    end_date: ''
};

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    initializeCharts();
    initializeTable();
    setDefaultDates();
    loadData();
    startAutoRefresh();
});

// Set default date range (last 7 days)
function setDefaultDates() {
    const today = new Date();
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(today.getDate() - 7);

    document.getElementById('start-date').value = sevenDaysAgo.toISOString().split('T')[0];
    document.getElementById('end-date').value = today.toISOString().split('T')[0];

    currentFilters.start_date = document.getElementById('start-date').value;
    currentFilters.end_date = document.getElementById('end-date').value;
}

// Initialize Chart.js charts
function initializeCharts() {
    // Crowd over time chart
    const crowdCtx = document.getElementById('crowdChart').getContext('2d');
    crowdChart = new Chart(crowdCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Average Crowd',
                data: [],
                borderColor: 'rgb(75, 192, 192)',
                backgroundColor: 'rgba(75, 192, 192, 0.2)',
                tension: 0.1
            }, {
                label: 'Peak Crowd',
                data: [],
                borderColor: 'rgb(255, 99, 132)',
                backgroundColor: 'rgba(255, 99, 132, 0.2)',
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Crowd Count'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time'
                    }
                }
            }
        }
    });

    // Location comparison chart
    const locationCtx = document.getElementById('locationChart').getContext('2d');
    locationChart = new Chart(locationCtx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Average Crowd',
                data: [],
                backgroundColor: [
                    'rgba(255, 99, 132, 0.8)',
                    'rgba(54, 162, 235, 0.8)',
                    'rgba(255, 205, 86, 0.8)'
                ],
                borderColor: [
                    'rgb(255, 99, 132)',
                    'rgb(54, 162, 235)',
                    'rgb(255, 205, 86)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: 1
                    },
                    title: {
                        display: true,
                        text: 'Average Crowd Count (people)'
                    }
                }
            }
        }
    });
}

// Initialize DataTable
function initializeTable() {
    dataTable = $('#crowdTable').DataTable({
        columns: [
            { data: 'id' },
            { data: 'timestamp' },
            { data: 'location_id' },
            { data: 'frame_id' },
            { data: 'crowd_count' },
            { data: 'created_at' }
        ],
        order: [[1, 'desc']], // Sort by timestamp descending
        pageLength: 25,
        responsive: true
    });
}

// Load all data
function loadData() {
    loadSummary();
    loadChartData();
    loadTableData();
    updateLastRefresh();
}

// Load summary statistics
function loadSummary() {
    fetch('/api/summary')
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                console.error('Error loading summary:', data.error);
                return;
            }

            document.getElementById('total-frames').textContent = data.total_frames || 0;
            document.getElementById('avg-crowd').textContent = Math.round(data.overall_avg_crowd || 0);
            document.getElementById('peak-crowd').textContent = data.overall_peak_crowd || 0;
            document.getElementById('total-locations').textContent = data.total_locations || 0;
        })
        .catch(error => console.error('Error loading summary:', error));
}

// Load chart data
function loadChartData() {
    // Load time series data
    fetch(`/api/chart-data?location_id=${currentFilters.location_id}&time_range=hour`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                console.error('Error loading chart data:', data.error);
                return;
            }

            updateCrowdChart(data);
        })
        .catch(error => console.error('Error loading chart data:', error));

    // Load location comparison data
    fetch(`/api/statistics?location_id=${currentFilters.location_id}&start_date=${currentFilters.start_date}&end_date=${currentFilters.end_date}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                console.error('Error loading location data:', data.error);
                return;
            }

            updateLocationChart(data);
        })
        .catch(error => console.error('Error loading location data:', error));
}

// Load table data
function loadTableData() {
    fetch('/api/latest?limit=100')
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                console.error('Error loading table data:', data.error);
                return;
            }

            dataTable.clear();
            dataTable.rows.add(data);
            dataTable.draw();
        })
        .catch(error => console.error('Error loading table data:', error));
}

// Update crowd over time chart
function updateCrowdChart(data) {
    const labels = [];
    const avgData = [];
    const peakData = [];

    // Group by time period
    const groupedData = {};
    data.forEach(item => {
        const timeKey = new Date(item.time_period).toLocaleString();
        if (!groupedData[timeKey]) {
            groupedData[timeKey] = { avg: [], peak: [] };
        }
        groupedData[timeKey].avg.push(item.avg_crowd);
        groupedData[timeKey].peak.push(item.peak_crowd);
    });

    // Calculate averages
    Object.keys(groupedData).sort().forEach(timeKey => {
        labels.push(timeKey);
        const avgValues = groupedData[timeKey].avg;
        const peakValues = groupedData[timeKey].peak;

        avgData.push(avgValues.reduce((a, b) => a + b, 0) / avgValues.length);
        peakData.push(Math.max(...peakValues));
    });

    crowdChart.data.labels = labels;
    crowdChart.data.datasets[0].data = avgData;
    crowdChart.data.datasets[1].data = peakData;
    crowdChart.update();
}

// Update location comparison chart
function updateLocationChart(data) {
    const locationData = {};
    data.forEach(item => {
        if (!locationData[item.location_id]) {
            locationData[item.location_id] = [];
        }
        locationData[item.location_id].push(item.avg_crowd);
    });

    const labels = Object.keys(locationData);
    const avgValues = labels.map(location => {
        const values = locationData[location];
        return Math.round(values.reduce((a, b) => a + b, 0) / values.length);
    });

    locationChart.data.labels = labels;
    locationChart.data.datasets[0].data = avgValues;
    locationChart.update();
}

// Apply filters
function applyFilters() {
    currentFilters.location_id = document.getElementById('location-filter').value;
    currentFilters.start_date = document.getElementById('start-date').value;
    currentFilters.end_date = document.getElementById('end-date').value;

    loadData();
}

// Auto refresh every 5 seconds
function startAutoRefresh() {
    setInterval(loadData, 5000);
}

// Update last refresh timestamp
function updateLastRefresh() {
    const now = new Date();
    const timeString = now.toLocaleTimeString('vi-VN');
    document.getElementById('last-update').textContent = `Cập nhật lần cuối: ${timeString}`;
}

// Load hourly statistics for report analysis
function loadHourlyStats() {
    const date = document.getElementById('hourly-date').value;
    const startHour = document.getElementById('hourly-start').value;
    const endHour = document.getElementById('hourly-end').value;
    
    if (!date) {
        alert('Please select a date');
        return;
    }
    
    // Convert Auburn time (PST = UTC-8) to UTC for database query
    const auburnStartHour = parseInt(startHour);
    const auburnEndHour = parseInt(endHour);
    const utcStartHour = auburnStartHour + 8;  // PST to UTC: +8 hours
    const utcEndHour = auburnEndHour + 8;
    
    // Handle day rollover
    const startDate = new Date(date);
    let endDate = new Date(date);
    
    // If end hour <= start hour, it means query spans to next day
    // Example: 12:00 → 00:00 means 12 hours (same day 12:00 to next day 00:00)
    // Example: 22:00 → 08:00 means 10 hours (22:00 today to 08:00 tomorrow)
    if (auburnEndHour <= auburnStartHour) {
        endDate.setDate(endDate.getDate() + 1);
    }
    
    // Adjust for UTC rollover (when PST hour + 8 >= 24)
    if (utcStartHour >= 24) {
        startDate.setDate(startDate.getDate() + 1);
    }
    if (utcEndHour >= 24) {
        endDate.setDate(endDate.getDate() + 1);
    }
    
    // Format for database query (UTC)
    const hourStartUTC = `${startDate.toISOString().split('T')[0]} ${String(utcStartHour % 24).padStart(2, '0')}:00:00`;
    const hourEndUTC = `${endDate.toISOString().split('T')[0]} ${String(utcEndHour % 24).padStart(2, '0')}:00:00`;
    
    // Format for display (Auburn local time)
    const displayStart = `${date} ${String(auburnStartHour).padStart(2, '0')}:00:00 PST`;
    const displayEndDate = endDate > startDate ? endDate.toISOString().split('T')[0] : date;
    const displayEnd = `${displayEndDate} ${String(auburnEndHour).padStart(2, '0')}:00:00 PST`;
    const displayUTC = `UTC: ${hourStartUTC} → ${hourEndUTC}`;
    
    fetch(`/api/hourly-stats?location_id=all&hour_start=${hourStartUTC}&hour_end=${hourEndUTC}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                console.error('Error loading hourly stats:', data.error);
                alert('Error loading hourly stats: ' + data.error);
                return;
            }
            
            displayHourlyResults(data, displayStart, displayEnd, displayUTC);
        })
        .catch(error => {
            console.error('Error loading hourly stats:', error);
            alert('Error loading hourly stats. Check console for details.');
        });
}

// Display hourly statistics results
function displayHourlyResults(data, hourStart, hourEnd, displayUTC) {
    const container = document.getElementById('hourly-results');
    
    if (data.length === 0) {
        container.innerHTML = '<div class="col-12"><p class="text-muted">No data available for this time period.</p></div>';
        return;
    }
    
    let html = `
        <div class="col-12 mb-3">
           
            <small class="text-muted">${displayUTC}</small>
        </div>
    `;
    // <h6 class="text-info">📅 ${hourStart} → ${hourEnd}</h6>

    data.forEach(loc => {
        const avgActiveDisplay = loc.avg_active !== null ? `${loc.avg_active} people` : 'N/A';
        const activeRateDisplay = loc.active_rate !== null ? `${loc.active_rate}%` : '0%';
        
        html += `
            <div class="col-md-6 mb-3">
                <div class="card bg-light border-secondary">
                    <div class="card-body">
                        <h6 class="card-title text-primary">${loc.location_id}</h6>
                        <hr>
                        
                        <!-- All Frames Stats -->
                        <div class="mb-3">
                            <p class="mb-2"><small class="text-muted">📊 <strong>All Frames</strong> (${loc.total_frames} total)</small></p>
                            <div class="row">
                                <div class="col-6">
                                    <p class="mb-0 text-muted" style="font-size: 0.85rem;">Average:</p>
                                    <h4 class="text-secondary mb-0">${loc.avg_all}</h4>
                                    <small class="text-muted">people</small>
                                </div>
                                <div class="col-6">
                                    <p class="mb-0 text-muted" style="font-size: 0.85rem;">Peak:</p>
                                    <h4 class="text-danger mb-0">${loc.peak_crowd}</h4>
                                    <small class="text-muted">people</small>
                                </div>
                            </div>
                        </div>
                        
                        <hr>
                        
                        <!-- Active Frames Stats -->
                        <div>
                            <p class="mb-2"><small class="text-muted">👥 <strong>Active Frames Only</strong> (${loc.active_frames} frames)</small></p>
                            <div class="row">
                                <div class="col-6">
                                    <p class="mb-0 text-muted" style="font-size: 0.85rem;">Average:</p>
                                    <h4 class="text-success mb-0">${avgActiveDisplay}</h4>
                                    <small class="text-muted">when present</small>
                                </div>
                                <div class="col-6">
                                    <p class="mb-0 text-muted" style="font-size: 0.85rem;">Active Rate:</p>
                                    <h4 class="text-info mb-0">${activeRateDisplay}</h4>
                                    <small class="text-muted">occupancy</small>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });
    
    container.innerHTML = html;
}
