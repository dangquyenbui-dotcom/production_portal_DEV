// Global state for sorting, defaulting to the most urgent items.
let sortState = {
    column: 'Due Ship',
    direction: 'asc',
    columnIndex: 7, // Index of the new "Due Ship" column
    columnType: 'date'
};

document.addEventListener('DOMContentLoaded', function() {
    // --- INITIALIZATION ---
    initializeEventListeners();
    initializeSorting(); // Set up sort handlers
    
    // 1. Populate filters with all possible options from the full dataset first.
    populateInitialFilterOptions();
    
    // 2. Now that all <option> elements exist, restore any saved selections.
    restoreFilters();
    
    // 3. Finally, run the filter to update the grid and apply the default sort.
    filterShortageTable();
});

function initializeEventListeners() {
    const searchInput = document.getElementById('shortageSearch');
    const exportButton = document.getElementById('exportBtn');
    const urgencyFilter = document.getElementById('urgencyFilter');
    const customerFilter = document.getElementById('customerFilter');
    const resetButton = document.getElementById('resetBtn');

    if (searchInput) {
        const debouncedSearch = dtUtils.debounce(filterShortageTable, 250);
        searchInput.addEventListener('keyup', debouncedSearch);
    }
    if (urgencyFilter) {
        urgencyFilter.addEventListener('change', filterShortageTable);
    }
    if (customerFilter) {
        customerFilter.addEventListener('change', filterShortageTable);
    }
    if (resetButton) {
        resetButton.addEventListener('click', resetFilters);
    }
    if (exportButton) {
        exportButton.addEventListener('click', exportVisibleDataToXlsx);
    }
}

function initializeSorting() {
    document.querySelectorAll('.table .sortable').forEach(th => {
        th.addEventListener('click', handleSort);
    });
}

function populateInitialFilterOptions() {
    const allRows = document.querySelectorAll('#shortageTableBody tr');
    const customerOptions = new Set();
    
    allRows.forEach(row => {
        const customers = (row.dataset.customers || '').split(',');
        customers.forEach(customer => {
            if (customer) customerOptions.add(customer);
        });
    });
    
    const customerSelect = document.getElementById('customerFilter');
    const sortedCustomers = Array.from(customerOptions).sort();
    sortedCustomers.forEach(customer => {
        const option = document.createElement('option');
        option.value = customer;
        option.textContent = customer;
        customerSelect.appendChild(option);
    });
}

function saveFilters() {
    const filters = {
        urgency: document.getElementById('urgencyFilter').value,
        customer: document.getElementById('customerFilter').value,
        search: document.getElementById('shortageSearch').value,
    };
    sessionStorage.setItem('buyerShortageFilters', JSON.stringify(filters));
}

function restoreFilters() {
    const savedFilters = JSON.parse(sessionStorage.getItem('buyerShortageFilters'));
    if (savedFilters) {
        document.getElementById('urgencyFilter').value = savedFilters.urgency || '15';
        document.getElementById('customerFilter').value = savedFilters.customer || '';
        document.getElementById('shortageSearch').value = savedFilters.search || '';
    } else {
        // If no saved filters, ensure the default is 15
        document.getElementById('urgencyFilter').value = '15';
    }
}

function resetFilters() {
    document.getElementById('urgencyFilter').value = '15';
    document.getElementById('customerFilter').value = '';
    document.getElementById('shortageSearch').value = '';
    
    sessionStorage.removeItem('buyerShortageFilters');
    filterShortageTable();
}

function updateRowCount(visibleRows, totalRows) {
    const rowCountEl = document.getElementById('rowCount');
    if (rowCountEl) {
        rowCountEl.textContent = `Showing ${visibleRows} of ${totalRows} rows`;
    }
}

function filterShortageTable() {
    const searchTerm = document.getElementById('shortageSearch').value.toLowerCase();
    const urgencyDays = document.getElementById('urgencyFilter').value;
    let customerFilter = document.getElementById('customerFilter').value;
    
    const allRows = document.querySelectorAll('#shortageTableBody tr');
    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const potentiallyVisibleRows = [];
    const availableCustomers = new Set();
    
    allRows.forEach(row => {
        let isPotentiallyVisible = true;
        
        if (urgencyDays !== 'all') {
            const dueDates = (row.dataset.dueDates || '').split(',');
            const daysThreshold = parseInt(urgencyDays, 10);
            let meetsUrgency = false;
            
            for (const dateStr of dueDates) {
                if (!dateStr) continue;
                const dateParts = dateStr.split('/'); 
                if (dateParts.length === 3) {
                    const dueDate = new Date(dateParts[2], dateParts[0] - 1, dateParts[1]);
                    const diffTime = dueDate - today;
                    const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
                    
                    if (diffDays >= 0 && diffDays <= daysThreshold) {
                        meetsUrgency = true;
                        break;
                    }
                }
            }
            if (!meetsUrgency) {
                isPotentiallyVisible = false;
            }
        }
        
        const partNumber = row.cells[0].textContent.toLowerCase();
        const description = row.cells[1].textContent.toLowerCase();
        const customersText = row.cells[2].textContent.toLowerCase();
        if (searchTerm && !(partNumber.includes(searchTerm) || description.includes(searchTerm) || customersText.includes(searchTerm))) {
            isPotentiallyVisible = false;
        }
        
        if (isPotentiallyVisible) {
            potentiallyVisibleRows.push(row);
            const customersInRow = (row.dataset.customers || '').split(',');
            customersInRow.forEach(c => {
                if(c) availableCustomers.add(c);
            });
        }
        
        row.style.display = 'none';
    });

    const customerSelect = document.getElementById('customerFilter');
    const currentCustomerValue = customerSelect.value;
    
    while (customerSelect.options.length > 1) {
        customerSelect.remove(1);
    }
    
    Array.from(availableCustomers).sort().forEach(customer => {
        const option = document.createElement('option');
        option.value = customer;
        option.textContent = customer;
        customerSelect.appendChild(option);
    });

    if (availableCustomers.has(currentCustomerValue)) {
        customerSelect.value = currentCustomerValue;
    } else {
        customerSelect.value = "";
    }
    
    let visibleRowCount = 0;
    const finalCustomerFilter = customerSelect.value;

    potentiallyVisibleRows.forEach(row => {
        const customers = (row.dataset.customers || '').split(',');
        if (!finalCustomerFilter || customers.includes(finalCustomerFilter)) {
            row.style.display = '';
            visibleRowCount++;
        }
    });

    updateRowCount(visibleRowCount, allRows.length);
    saveFilters();
    sortTable();
    updateSortIndicators();
}

function handleSort(e) {
    const th = e.currentTarget;
    const columnId = th.dataset.columnId;
    const columnType = th.dataset.type || 'string';
    const columnIndex = Array.from(th.parentElement.children).indexOf(th);

    if (sortState.column === columnId) {
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.column = columnId;
        sortState.direction = 'asc';
    }
    sortState.columnIndex = columnIndex;
    sortState.columnType = columnType;

    sortTable();
    updateSortIndicators();
}

function updateSortIndicators() {
    document.querySelectorAll('.table .sortable').forEach(th => {
        const indicator = th.querySelector('.sort-indicator');
        if (!indicator) return;

        th.classList.remove('sorted-asc', 'sorted-desc');
        indicator.textContent = '';

        if (th.dataset.columnId === sortState.column) {
            th.classList.add(sortState.direction === 'asc' ? 'sorted-asc' : 'sorted-desc');
            indicator.textContent = sortState.direction === 'asc' ? '↑' : '↓';
        }
    });
}

function getSortValue(cell, type) {
    if (!cell) return type === 'numeric' ? -Infinity : (type === 'date' ? new Date('2999-12-31') : ''); // Sort empty dates last
    const text = cell.textContent.trim();
    if (!text || text === 'N/A') return type === 'numeric' ? -Infinity : (type === 'date' ? new Date('2999-12-31') : '');

    switch (type) {
        case 'numeric':
            return parseFloat(text.replace(/,/g, '')) || 0;
        case 'date':
            const parts = text.split('/');
            if (parts.length === 3) {
                return new Date(parts[2], parts[0] - 1, parts[1]);
            }
            return new Date('2999-12-31'); // Sort invalid dates last
        default:
            return text.toLowerCase();
    }
}

function sortTable() {
    if (!sortState.column) return;
    
    const tbody = document.getElementById('shortageTableBody');
    const rows = Array.from(tbody.querySelectorAll('tr'));

    rows.sort((a, b) => {
        const valA = getSortValue(a.cells[sortState.columnIndex], sortState.columnType);
        const valB = getSortValue(b.cells[sortState.columnIndex], sortState.columnType);
        
        let comparison = 0;
        if (valA > valB) {
            comparison = 1;
        } else if (valA < valB) {
            comparison = -1;
        }

        return sortState.direction === 'asc' ? comparison : -comparison;
    });

    rows.forEach(row => tbody.appendChild(row));
}

function exportVisibleDataToXlsx() {
    const exportBtn = document.getElementById('exportBtn');
    exportBtn.disabled = true;
    exportBtn.textContent = '📥 Generating...';

    const headers = Array.from(document.querySelectorAll('.table thead th'))
        .map(th => th.textContent.trim());

    const rows = [];
    document.querySelectorAll('#shortageTableBody tr:not([style*="display: none"])').forEach(row => {
        const rowData = Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim());
        rows.push(rowData);
    });

    if (rows.length === 0) {
        dtUtils.showAlert('No data to export.', 'info');
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
        return;
    }
    
    fetch('/mrp/api/export-shortages-xlsx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ headers, rows })
    })
    .then(response => {
        if (!response.ok) { throw new Error('Network response was not ok.'); }
        const disposition = response.headers.get('Content-Disposition');
        const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(disposition);
        const filename = (matches != null && matches[1]) ? matches[1].replace(/['"]/g, '') : 'mrp_shortage_report.xlsx';
        return Promise.all([response.blob(), filename]);
    })
    .then(([blob, filename]) => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
    })
    .catch(error => {
        console.error('Export error:', error);
        dtUtils.showAlert('An error occurred during the export.', 'error');
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
    });
}