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
                    title: {
                        display: true,
                        text: 'Average Crowd Count'
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
    fetch(`/api/statistics?location_id=all&start_date=${currentFilters.start_date}&end_date=${currentFilters.end_date}`)
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
        return values.reduce((a, b) => a + b, 0) / values.length;
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
