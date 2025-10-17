// static/js/scheduling.js

document.addEventListener('DOMContentLoaded', function() {
    
    // --- INITIALIZATION ---
    initializeColumnToggle(); // Set up column toggling first
    initializeSorting();      // NEW: Set up sorting
    attachAllEventListeners();
    
    // 1. Populate filters with all possible options from the full dataset first.
    updateFilterOptions(); 
    
    // 2. Now that all <option> and checkbox elements exist, restore the saved selections.
    restoreFilters();      
    
    // 3. Finally, run the filter to update the grid view based on the restored state.
    filterGrid();          
    
    updateLastUpdatedTime();

    if (sessionStorage.getItem('wasRefreshed')) {
        dtUtils.showAlert('Data refreshed successfully!', 'success');
        sessionStorage.removeItem('wasRefreshed');
    }
});

// --- EVENT LISTENERS ---
function attachAllEventListeners() {
    // Single-select dropdowns
    document.getElementById('buFilter').addEventListener('change', filterGrid);
    document.getElementById('customerFilter').addEventListener('change', filterGrid);

    // Buttons
    document.getElementById('exportBtn').addEventListener('click', exportVisibleDataToXlsx);
    document.getElementById('resetBtn').addEventListener('click', resetFilters);
    document.getElementById('refreshBtn').addEventListener('click', () => {
        saveFilters(); 
        sessionStorage.setItem('wasRefreshed', 'true');
        window.location.reload();
    });

    // Multi-select event listeners
    setupMultiSelect('facilityFilter');
    setupMultiSelect('soTypeFilter');
    setupMultiSelect('dueShipFilter'); // ADDED

    attachEditableListeners(document.getElementById('schedule-body'));
}

function setupMultiSelect(baseId) {
    const btn = document.getElementById(`${baseId}Btn`);
    const dropdown = document.getElementById(`${baseId}Dropdown`);

    btn.addEventListener('click', (e) => {
        e.stopPropagation();
        // Close other dropdowns
        document.querySelectorAll('.multiselect-dropdown.show').forEach(d => {
            if (d.id !== dropdown.id) d.classList.remove('show');
        });
        dropdown.classList.toggle('show');
    });

    dropdown.addEventListener('change', (e) => {
        if (e.target.type === 'checkbox') {
            updateMultiSelectButtonText(baseId);
            filterGrid();
        }
    });
}


// --- FILTER PERSISTENCE ---
function saveFilters() {
    const selectedSoTypes = Array.from(document.querySelectorAll('#soTypeFilterDropdown input:checked')).map(cb => cb.value);
    const selectedFacilities = Array.from(document.querySelectorAll('#facilityFilterDropdown input:checked')).map(cb => cb.value);
    const selectedDueShip = Array.from(document.querySelectorAll('#dueShipFilterDropdown input:checked')).map(cb => cb.value); // ADDED
    
    const filters = {
        facility: selectedFacilities,
        bu: document.getElementById('buFilter').value,
        soType: selectedSoTypes,
        customer: document.getElementById('customerFilter').value,
        dueShip: selectedDueShip, // MODIFIED
    };
    sessionStorage.setItem('schedulingFilters', JSON.stringify(filters));
}

function restoreFilters() {
    const savedFilters = JSON.parse(sessionStorage.getItem('schedulingFilters'));
    if (savedFilters) {
        document.getElementById('buFilter').value = savedFilters.bu || '';
        document.getElementById('customerFilter').value = savedFilters.customer || '';
        
        // Restore Multi-selects
        restoreMultiSelect('soTypeFilter', savedFilters.soType);
        restoreMultiSelect('facilityFilter', savedFilters.facility);
        restoreMultiSelect('dueShipFilter', savedFilters.dueShip); // MODIFIED
    }
}

function restoreMultiSelect(baseId, values) {
    const dropdown = document.getElementById(`${baseId}Dropdown`);
    dropdown.querySelectorAll('input').forEach(cb => cb.checked = false);
    if (values && values.length > 0) {
        values.forEach(value => {
            const checkbox = dropdown.querySelector(`input[value="${value}"]`);
            if (checkbox) checkbox.checked = true;
        });
    }
    updateMultiSelectButtonText(baseId);
}

function resetFilters() {
    document.getElementById('buFilter').value = '';
    document.getElementById('customerFilter').value = '';
    
    // Reset multi-select filters
    ['soTypeFilter', 'facilityFilter', 'dueShipFilter'].forEach(baseId => { // MODIFIED
        document.querySelectorAll(`#${baseId}Dropdown input:checked`).forEach(cb => cb.checked = false);
        updateMultiSelectButtonText(baseId);
    });

    sessionStorage.removeItem('schedulingFilters');
    filterGrid();
}


// --- UI, FILTERING & TOTALS ---
function updateLastUpdatedTime() {
    const timestampEl = document.getElementById('lastUpdated');
    if (timestampEl) {
        const now = new Date();
        const timeString = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        timestampEl.textContent = `Last Updated: ${timeString}`;
    }
}

function calculateTotals() {
    let totalNoLowRisk = 0;
    let totalHighRisk = 0;

    document.querySelectorAll('#schedule-body tr:not(.hidden-row)').forEach(row => {
        const noLowRiskCell = row.querySelector('[data-calculated-for="No/Low Risk Qty"]');
        const highRiskCell = row.querySelector('[data-calculated-for="High Risk Qty"]');

        if (noLowRiskCell) totalNoLowRisk += parseFloat(noLowRiskCell.textContent.replace(/[$,]/g, '')) || 0;
        if (highRiskCell) totalHighRisk += parseFloat(highRiskCell.textContent.replace(/[$,]/g, '')) || 0;
    });

    document.getElementById('total-no-low-risk').textContent = totalNoLowRisk.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
    document.getElementById('total-high-risk').textContent = totalHighRisk.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
    
    updateForecastCards(totalNoLowRisk, totalHighRisk);
}

function updateForecastCards(totalNoLowRisk, totalHighRisk) {
    const getValueFromCardById = (elementId) => {
        const cardElement = document.getElementById(elementId);
        if (!cardElement) return 0;
        return parseFloat(cardElement.textContent.replace(/[$,]/g, '')) || 0;
    };

    const shippedCurrentMonth = getValueFromCardById('shipped-as-value');
    const fgBefore = getValueFromCardById('fg-on-hand-before');
    const fgCurrent = getValueFromCardById('fg-on-hand-current');
    const fgFuture = getValueFromCardById('fg-on-hand-future');

    const forecastLikelyValue = shippedCurrentMonth + totalNoLowRisk + fgBefore + fgCurrent;
    const forecastMaybeValue = shippedCurrentMonth + totalNoLowRisk + totalHighRisk + fgBefore + fgCurrent + fgFuture;

    document.getElementById('forecast-likely-value').textContent = forecastLikelyValue.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
    document.getElementById('forecast-maybe-value').textContent = forecastMaybeValue.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
}

function updateRowCount() {
    const totalRows = document.querySelectorAll('#schedule-body tr[data-so-number]').length;
    const visibleRows = document.querySelectorAll('#schedule-body tr:not(.hidden-row)').length;
    const rowCountEl = document.getElementById('rowCount');
    if (rowCountEl) {
         if (totalRows === 0 && document.querySelector('#schedule-body td[colspan]')) {
             rowCountEl.textContent = `Showing 0 of 0 rows`;
        } else {
             rowCountEl.textContent = `Showing ${visibleRows} of ${totalRows} rows`;
        }
    }
}

function populateSelect(selectId, options, addBlankOption = false, selectedValue = null) {
    const select = document.getElementById(selectId);
    if (!select) return;

    select.innerHTML = `<option value="">All</option>`;
    options.forEach(optionText => {
        if (optionText) {
            const option = document.createElement('option');
            option.value = optionText;
            option.textContent = optionText;
            select.appendChild(option);
        }
    });
    if (addBlankOption) {
        const blankOption = document.createElement('option');
        blankOption.value = 'Blank';
        blankOption.textContent = 'Blank';
        select.appendChild(blankOption);
    }
    
    if (selectedValue) {
        const optionExists = Array.from(select.options).some(opt => opt.value === selectedValue);
        if (optionExists) {
            select.value = selectedValue;
        }
    }
}

function populateMultiSelect(baseId, options, addBlankOption = false) { // MODIFIED
    const dropdown = document.getElementById(`${baseId}Dropdown`);
    if (!dropdown) return;

    const savedValues = Array.from(dropdown.querySelectorAll('input:checked')).map(cb => cb.value);
    dropdown.innerHTML = '';

    options.forEach(optionText => {
        if (optionText) {
            const label = document.createElement('label');
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.value = optionText;
            if (savedValues.includes(optionText)) {
                checkbox.checked = true;
            }
            label.appendChild(checkbox);
            label.appendChild(document.createTextNode(' ' + optionText));
            dropdown.appendChild(label);
        }
    });
    
    if (addBlankOption) { // ADDED BLOCK
        const label = document.createElement('label');
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = 'Blank';
        if (savedValues.includes('Blank')) {
            checkbox.checked = true;
        }
        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(' (No Date)'));
        dropdown.appendChild(label);
    }
}


function updateMultiSelectButtonText(baseId) {
    const dropdown = document.getElementById(`${baseId}Dropdown`);
    const selected = Array.from(dropdown.querySelectorAll('input:checked'));
    const btn = document.getElementById(`${baseId}Btn`);
    if (selected.length === 0 || selected.length === dropdown.querySelectorAll('input').length) {
        btn.textContent = 'All';
    } else if (selected.length === 1) {
        btn.textContent = selected[0].value;
    } else {
        btn.textContent = `${selected.length} selected`;
    }
}


function updateFilterOptions() {
    const selectedBU = document.getElementById('buFilter').value;
    const selectedCustomer = document.getElementById('customerFilter').value;
    const selectedDueDate = Array.from(document.querySelectorAll('#dueShipFilterDropdown input:checked')).map(cb => cb.value); // MODIFIED
    
    const selectedSoTypes = Array.from(document.querySelectorAll('#soTypeFilterDropdown input:checked')).map(cb => cb.value);
    const selectedFacilities = Array.from(document.querySelectorAll('#facilityFilterDropdown input:checked')).map(cb => cb.value);

    const allRows = document.getElementById('schedule-body').querySelectorAll('tr');
    
    const getOptionsFor = (filterToUpdate) => {
        const options = new Set();
        let hasBlank = false;
        
        allRows.forEach(row => {
            if (row.cells.length < 5) return;
            
            const facility = row.querySelector('[data-field="Facility"]')?.textContent || '';
            const bu = row.querySelector('[data-field="BU"]')?.textContent || '';
            const soType = row.querySelector('[data-field="SO Type"]')?.textContent || '';
            const customer = row.querySelector('[data-field="Customer Name"]')?.textContent || '';
            const dueDate = row.querySelector('[data-field="Due to Ship"]')?.textContent.trim() || '';
            const dueDateMonthYear = (dueDate && dueDate.includes('/')) ? `${dueDate.split('/')[0].padStart(2, '0')}/${dueDate.split('/')[2]}` : 'Blank';

            let matches = true;
            if (filterToUpdate !== 'facility' && selectedFacilities.length > 0 && !selectedFacilities.includes(facility)) matches = false;
            if (filterToUpdate !== 'bu' && selectedBU && bu !== selectedBU) matches = false;
            if (filterToUpdate !== 'soType' && selectedSoTypes.length > 0 && !selectedSoTypes.includes(soType)) matches = false;
            if (filterToUpdate !== 'customer' && selectedCustomer && customer !== selectedCustomer) matches = false;
            if (filterToUpdate !== 'dueShip' && selectedDueDate.length > 0) { // MODIFIED
                if (dueDate === '' && !selectedDueDate.includes('Blank')) matches = false;
                else if (dueDate !== '' && !selectedDueDate.includes(dueDateMonthYear)) matches = false;
            }

            if (matches) {
                switch(filterToUpdate) {
                    case 'facility': options.add(facility); break;
                    case 'bu': options.add(bu); break;
                    case 'soType': options.add(soType); break;
                    case 'customer': options.add(customer); break;
                    case 'dueShip': 
                        if (dueDateMonthYear === 'Blank') hasBlank = true;
                        else options.add(dueDateMonthYear);
                        break;
                }
            }
        });
        return { options: [...options].sort(), hasBlank };
    };
    
    const facilityOpts = getOptionsFor('facility');
    const buOpts = getOptionsFor('bu');
    const soTypeOpts = getOptionsFor('soType');
    const customerOpts = getOptionsFor('customer');
    const dueDateOpts = getOptionsFor('dueShip');

    const sortedDueDates = dueDateOpts.options.sort((a, b) => {
        const [aMonth, aYear] = a.split('/');
        const [bMonth, bYear] = b.split('/');
        return new Date(aYear, aMonth - 1) - new Date(bYear, bMonth - 1);
    });

    populateMultiSelect('facilityFilter', facilityOpts.options);
    populateSelect('buFilter', buOpts.options, false, selectedBU);
    populateMultiSelect('soTypeFilter', soTypeOpts.options);
    populateSelect('customerFilter', customerOpts.options, false, selectedCustomer);
    populateMultiSelect('dueShipFilter', sortedDueDates, dueDateOpts.hasBlank); // MODIFIED
    
    updateMultiSelectButtonText('facilityFilter');
    updateMultiSelectButtonText('soTypeFilter');
    updateMultiSelectButtonText('dueShipFilter'); // ADDED
}

function filterGrid() {
    const facilityFilter = Array.from(document.querySelectorAll('#facilityFilterDropdown input:checked')).map(cb => cb.value);
    const buFilter = document.getElementById('buFilter').value;
    const soTypeFilter = Array.from(document.querySelectorAll('#soTypeFilterDropdown input:checked')).map(cb => cb.value);
    const customerFilter = document.getElementById('customerFilter').value;
    const dueShipFilter = Array.from(document.querySelectorAll('#dueShipFilterDropdown input:checked')).map(cb => cb.value); // MODIFIED

    document.getElementById('schedule-body').querySelectorAll('tr').forEach(row => {
        if (row.cells.length < 2) return;
        const facility = row.querySelector('[data-field="Facility"]')?.textContent || '';
        const bu = row.querySelector('[data-field="BU"]')?.textContent || '';
        const soType = row.querySelector('[data-field="SO Type"]')?.textContent || '';
        const customer = row.querySelector('[data-field="Customer Name"]')?.textContent || '';
        const dueDate = row.querySelector('[data-field="Due to Ship"]')?.textContent.trim() || '';
        
        let show = true;
        if (facilityFilter.length > 0 && !facilityFilter.includes(facility)) show = false;
        if (buFilter && bu !== buFilter) show = false;
        if (soTypeFilter.length > 0 && !soTypeFilter.includes(soType)) show = false;
        if (customerFilter && customer !== customerFilter) show = false;
        
        // MODIFIED BLOCK for dueShipFilter
        if (dueShipFilter.length > 0) {
            const dueDateMonthYear = (dueDate && dueDate.includes('/')) ? `${dueDate.split('/')[0].padStart(2, '0')}/${dueDate.split('/')[2]}` : 'Blank';
            if (!dueShipFilter.includes(dueDateMonthYear)) {
                show = false;
            }
        }
        
        row.classList.toggle('hidden-row', !show);
    });
    
    saveFilters();
    updateFilterOptions();
    updateRowCount();
    calculateTotals();
    validateAllRows();
}

// --- COLUMN TOGGLE LOGIC (UNCHANGED) ---
const COLUMNS_CONFIG_KEY = 'schedulingColumnConfig';

function initializeColumnToggle() {
    const dropdown = document.getElementById('column-dropdown');
    const headers = document.querySelectorAll('.grid-table thead th');
    let savedConfig = JSON.parse(localStorage.getItem(COLUMNS_CONFIG_KEY));

    if (!savedConfig) {
        savedConfig = {};
        headers.forEach(th => {
            const id = th.dataset.columnId;
            if (id) {
                const defaultHidden = ['Ord Qty - (00) Level', 'Total Shipped Qty', 'Produced Qty', 'ERP Can Make', 'ERP Low Risk', 'ERP High Risk', 'Unit Price', 'Qty Per UoM', 'Sales Rep'];
                savedConfig[id] = !defaultHidden.includes(id);
            }
        });
    }
    
    headers.forEach(th => {
        const id = th.dataset.columnId;
        if (id) {
            const isVisible = savedConfig[id] !== false;
            const label = document.createElement('label');
            label.innerHTML = `<input type="checkbox" data-column-id="${id}" ${isVisible ? 'checked' : ''}> ${id}`;
            dropdown.appendChild(label);

            label.querySelector('input').addEventListener('change', handleColumnToggle);
        }
    });

    applyColumnVisibility(savedConfig);

    const columnsBtn = document.getElementById('columnsBtn');
    columnsBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        dropdown.classList.toggle('show');
    });

    document.addEventListener('click', (e) => {
        if (!dropdown.contains(e.target) && !columnsBtn.contains(e.target)) {
            dropdown.classList.remove('show');
        }
    });
}

function handleColumnToggle(e) {
    const columnId = e.target.dataset.columnId;
    const isVisible = e.target.checked;
    
    let savedConfig = JSON.parse(localStorage.getItem(COLUMNS_CONFIG_KEY)) || {};
    savedConfig[columnId] = isVisible;
    localStorage.setItem(COLUMNS_CONFIG_KEY, JSON.stringify(savedConfig));
    
    applyColumnVisibility(savedConfig);
}

function applyColumnVisibility(config) {
    const table = document.querySelector('.grid-table');
    const headers = Array.from(table.querySelectorAll('thead th'));

    for (const columnId in config) {
        const isVisible = config[columnId];
        const headerIndex = headers.findIndex(th => th.dataset.columnId === columnId);

        if (headerIndex > -1) {
            const displayStyle = isVisible ? '' : 'none';
            table.querySelector(`th[data-column-id="${columnId}"]`).style.display = displayStyle;
            table.querySelectorAll(`tbody tr`).forEach(row => {
                if (row.cells[headerIndex]) {
                    row.cells[headerIndex].style.display = displayStyle;
                }
            });
        }
    }
}

// --- SORTING LOGIC (UNCHANGED) ---
let sortState = {
    column: null,
    direction: 'none'
};

function initializeSorting() {
    document.querySelectorAll('.grid-table .sortable').forEach(th => {
        th.addEventListener('click', handleSort);
    });
}

function handleSort(e) {
    const th = e.currentTarget;
    const columnId = th.dataset.columnId;
    const columnType = th.dataset.type || 'string';
    const columnIndex = Array.from(th.parentElement.children).indexOf(th);

    let newDirection;
    if (sortState.column === columnId) {
        if (sortState.direction === 'asc') newDirection = 'desc';
        else newDirection = 'asc';
    } else {
        newDirection = 'asc';
    }

    sortState.column = columnId;
    sortState.direction = newDirection;

    sortTable(columnIndex, columnType, newDirection);
    updateSortIndicators();
}

function updateSortIndicators() {
    document.querySelectorAll('.grid-table .sortable').forEach(th => {
        const indicator = th.querySelector('.sort-indicator');
        if (!indicator) return;

        th.classList.remove('sorted-asc', 'sorted-desc');
        indicator.textContent = '';

        if (th.dataset.columnId === sortState.column) {
            if (sortState.direction === 'asc') {
                th.classList.add('sorted-asc');
                indicator.textContent = '↑';
            } else if (sortState.direction === 'desc') {
                th.classList.add('sorted-desc');
                indicator.textContent = '↓';
            }
        }
    });
}

function getSortValue(cell, type) {
    if (!cell) return null;
    const text = cell.textContent.trim();

    switch (type) {
        case 'numeric':
            return parseFloat(text.replace(/[$,]/g, '')) || 0;
        case 'date':
            if (!text || !text.includes('/')) return new Date(0);
            const parts = text.split('/');
            return new Date(`20${parts[2]}`, parts[0] - 1, parts[1]);
        default:
            return text.toLowerCase();
    }
}

function sortTable(columnIndex, columnType, direction) {
    const tbody = document.getElementById('schedule-body');
    const rows = Array.from(tbody.querySelectorAll('tr'));

    rows.sort((a, b) => {
        const valA = getSortValue(a.cells[columnIndex], columnType);
        const valB = getSortValue(b.cells[columnIndex], columnType);
        
        let comparison = 0;
        if (valA > valB) {
            comparison = 1;
        } else if (valA < valB) {
            comparison = -1;
        }

        return direction === 'asc' ? comparison : -comparison;
    });

    tbody.innerHTML = '';
    rows.forEach(row => tbody.appendChild(row));
    
    attachEditableListeners(tbody);
}


// --- VALIDATION & SUGGESTION LOGIC (UNCHANGED) ---
function validateAllRows() {
    document.querySelectorAll('#schedule-body tr:not(.hidden-row)').forEach(validateRow);
}

function validateRow(row) {
    row.classList.remove('row-warning');
    const existingFix = row.querySelector('.suggestion-fix');
    if (existingFix) existingFix.remove();

    const netQtyCell = row.querySelector('[data-field="Net Qty"]');
    const noLowRiskCell = row.querySelector('[data-risk-type="No/Low Risk Qty"]');
    const highRiskCell = row.querySelector('[data-risk-type="High Risk Qty"]');

    if (!netQtyCell || !noLowRiskCell || !highRiskCell) return;

    const netQty = parseFloat(netQtyCell.textContent.replace(/,/g, '')) || 0;
    const noLowRiskQty = parseFloat(noLowRiskCell.textContent.replace(/,/g, '')) || 0;
    const highRiskQty = parseFloat(highRiskCell.textContent.replace(/,/g, '')) || 0;

    const totalProjected = noLowRiskQty + highRiskQty;
    const difference = totalProjected - netQty;

    if (Math.abs(difference) > 0.01) {
        row.classList.add('row-warning');
        const suggestedNoLowRisk = Math.max(0, noLowRiskQty - difference);
        const fixButton = document.createElement('button');
        fixButton.textContent = 'Fix';
        fixButton.dataset.suggestion = suggestedNoLowRisk;
        fixButton.onclick = function() { applySuggestion(this); };

        if (difference < 0) {
            fixButton.className = 'suggestion-fix fix-shortfall';
            fixButton.title = `SHORTFALL: Suggest setting No/Low Risk to ${suggestedNoLowRisk.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} to match Net Qty`;
            noLowRiskCell.appendChild(fixButton);
        } else {
            fixButton.className = 'suggestion-fix fix-surplus';
            fixButton.title = `SURPLUS: Suggest setting No/Low Risk to ${suggestedNoLowRisk.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})} to match Net Qty`;
            noLowRiskCell.appendChild(fixButton);
        }
    }
}


function applySuggestion(buttonElement) {
    const suggestion = parseFloat(buttonElement.dataset.suggestion);
    const cell = buttonElement.closest('td');

    if (cell) {
        const originalValue = cell.getAttribute('data-original-value') || '0';
        cell.textContent = suggestion.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const statusIndicator = document.createElement('span');
        statusIndicator.className = 'status-indicator saving';
        cell.appendChild(statusIndicator);

        const row = cell.closest('tr');
        const soNumber = row.dataset.soNumber;
        const partNumber = row.dataset.partNumber;
        const riskType = cell.dataset.riskType;
        const price = parseFloat(cell.dataset.price) || 0;
        const payload = { so_number: soNumber, part_number: partNumber, risk_type: riskType, quantity: suggestion };

        fetch('/scheduling/api/update-projection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(response => {
            if (!response.ok) { throw new Error(`HTTP error ${response.status}`); }
            return response.json();
        })
        .then(data => {
            if (data.success) {
                statusIndicator.className = 'status-indicator success';
                cell.setAttribute('data-original-value', suggestion.toString());
                const calculatedCell = row.querySelector(`[data-calculated-for="${riskType}"]`);
                if (calculatedCell) {
                    const newDollarValue = suggestion * price;
                    calculatedCell.textContent = newDollarValue.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
                }
                
                calculateTotals();
                validateRow(row);
                
                setTimeout(() => { statusIndicator.remove(); }, 2000);
            } else {
                throw new Error(data.message || 'Save failed.');
            }
        })
        .catch(error => {
            console.error('Save Error:', error);
            dtUtils.showAlert(`Save failed: ${error.message}`, 'error');
            cell.textContent = (parseFloat(originalValue) || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            statusIndicator.className = 'status-indicator error';
            validateRow(row);
        });
    }
}

// --- EDITABLE CELL LOGIC (UNCHANGED) ---
function attachEditableListeners(scope) {
    scope.querySelectorAll('.editable:not(.view-only)').forEach(cell => {
        cell.addEventListener('blur', handleCellBlur);
        cell.addEventListener('focus', handleCellFocus);
        cell.addEventListener('keydown', handleCellKeyDown);
    });
}

function handleCellBlur() {
    const el = this;
    el.querySelectorAll('.status-indicator').forEach(indicator => indicator.remove());
    const originalValue = el.getAttribute('data-original-value') || '0';
    let newValue = el.textContent.trim().replace(/[$,]/g, '');

    if (isNaN(newValue) || newValue.trim() === '') {
        el.textContent = (parseFloat(originalValue) || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        dtUtils.showAlert('Please enter a valid number.', 'error');
        return;
    }
    const quantity = parseFloat(newValue);
    el.textContent = quantity.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    
    validateRow(el.closest('tr'));

    if (Math.abs(parseFloat(originalValue) - quantity) < 0.001) return;

    const statusIndicator = document.createElement('span');
    statusIndicator.className = 'status-indicator saving';
    el.appendChild(statusIndicator);
    
    const row = el.closest('tr');
    const soNumber = row.dataset.soNumber;
    const partNumber = row.dataset.partNumber;
    const riskType = el.dataset.riskType;
    const price = parseFloat(el.dataset.price) || 0;

    const payload = { so_number: soNumber, part_number: partNumber, risk_type: riskType, quantity: quantity };

    fetch('/scheduling/api/update-projection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
    .then(response => {
        if (!response.ok) { throw new Error(`HTTP error ${response.status}`); }
        return response.json();
    })
    .then(data => {
        if (data.success) {
            statusIndicator.className = 'status-indicator success';
            el.setAttribute('data-original-value', quantity.toString());

            const calculatedCell = row.querySelector(`[data-calculated-for="${riskType}"]`);
            if (calculatedCell) {
                const newDollarValue = quantity * price;
                calculatedCell.textContent = newDollarValue.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
            }
            calculateTotals();
            setTimeout(() => { statusIndicator.remove(); }, 2000);
        } else {
            throw new Error(data.message || 'Save failed.');
        }
    })
    .catch(error => {
        console.error('Save Error:', error);
        statusIndicator.className = 'status-indicator error';
        el.textContent = (parseFloat(originalValue) || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const errorIndicator = document.createElement('span');
        errorIndicator.className = 'status-indicator error';
        el.appendChild(errorIndicator);
        dtUtils.showAlert(`Save failed: ${error.message}`, 'error');
    });
}

function handleCellFocus(e) {
    const cleanValue = e.target.textContent.trim().replace(/[$,]/g, '');
    e.target.setAttribute('data-original-value', cleanValue);
    e.target.querySelectorAll('.suggestion-fix').forEach(btn => btn.remove());
}

function handleCellKeyDown(e) {
    if (!/[\d.]/.test(e.key) && !['Backspace', 'Delete', 'ArrowLeft', 'ArrowRight', 'Tab', 'Enter'].includes(e.key)) { e.preventDefault(); }
    if (e.key === 'Enter') { e.preventDefault(); e.target.blur(); }
}

// --- EXPORT LOGIC (UNCHANGED) ---
function exportVisibleDataToXlsx() {
    const exportBtn = document.getElementById('exportBtn');
    exportBtn.disabled = true;
    exportBtn.textContent = '📥 Generating...';

    const headers = Array.from(document.querySelectorAll('.grid-table thead th'))
        .filter(th => th.style.display !== 'none')
        .map(th => th.textContent.trim());

    const rows = [];
    document.querySelectorAll('#schedule-body tr:not(.hidden-row)').forEach(row => {
        const rowData = [];
        row.querySelectorAll('td').forEach(cell => {
            if (cell.style.display !== 'none' && cell.getAttribute('data-field') !== 'SO Type') {
                rowData.push(cell.textContent.trim());
            }
        });
        rows.push(rowData);
    });

    if (rows.length === 0) {
        dtUtils.showAlert('No data to export.', 'info');
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
        return;
    }
    
    fetch('/scheduling/api/export-xlsx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ headers, rows })
    })
    .then(response => {
        if (!response.ok) { throw new Error('Network response was not ok.'); }
        const disposition = response.headers.get('Content-Disposition');
        const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(disposition);
        const filename = (matches != null && matches[1]) ? matches[1].replace(/['"]/g, '') : 'schedule_export.xlsx';
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