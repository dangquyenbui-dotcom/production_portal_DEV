// static/js/sales.js

document.addEventListener('DOMContentLoaded', function() {
    // Check if chart data is available (passed from the template)
    if (typeof salesTrendData !== 'undefined' && typeof topProductsData !== 'undefined') {
        const isDarkMode = document.documentElement.getAttribute('data-theme') === 'dark';
        const textColor = isDarkMode ? 'rgba(255, 255, 255, 0.8)' : 'rgba(0, 0, 0, 0.7)';
        const gridColor = isDarkMode ? 'rgba(255, 255, 255, 0.2)' : 'rgba(0, 0, 0, 0.1)';

        Chart.defaults.color = textColor;
        Chart.defaults.borderColor = gridColor;

        // Sales Trend Chart (Line)
        const salesCtx = document.getElementById('salesTrendChart').getContext('2d');
        new Chart(salesCtx, {
            type: 'line',
            data: {
                labels: salesTrendData.labels,
                datasets: [{
                    label: 'Monthly Sales',
                    data: salesTrendData.data,
                    backgroundColor: 'rgba(102, 126, 234, 0.2)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: { y: { beginAtZero: true } }
            }
        });

        // Top Products Chart (Bar)
        const productsCtx = document.getElementById('topProductsChart').getContext('2d');
        new Chart(productsCtx, {
            type: 'bar',
            data: {
                labels: topProductsData.map(item => item[0]),
                datasets: [{
                    label: 'Open Order Value',
                    data: topProductsData.map(item => item[1]),
                    backgroundColor: [
                        'rgba(102, 126, 234, 0.6)',
                        'rgba(72, 187, 120, 0.6)',
                        'rgba(237, 137, 54, 0.6)',
                        'rgba(128, 90, 213, 0.6)',
                        'rgba(245, 101, 101, 0.6)'
                    ],
                    borderColor: [
                        'rgba(102, 126, 234, 1)',
                        'rgba(72, 187, 120, 1)',
                        'rgba(237, 137, 54, 1)',
                        'rgba(128, 90, 213, 1)',
                        'rgba(245, 101, 101, 1)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                indexAxis: 'y',
                plugins: { legend: { display: false } },
                scales: { x: { beginAtZero: true } }
            }
        });
    }
});