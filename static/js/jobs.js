// static/js/jobs.js

let refreshIntervalId = null;
const REFRESH_INTERVAL_MS = 30000; // Refresh every 30 seconds
const LIVE_UPDATE_KEY = 'jobsLiveUpdateEnabled';
const FILTER_STORAGE_KEY = 'jobsFilters'; // Key for sessionStorage
const SORT_STORAGE_KEY = 'jobsSortState'; // Key for sort state
const EXPANDED_JOBS_KEY = 'jobsExpandedState'; // --- NEW: Key for expanded state ---

// Default Sorting State
let sortState = {
    column: 'job',
    direction: 'asc',
    columnIndex: 0,
    columnType: 'string'
};

document.addEventListener('DOMContentLoaded', function() {
    console.log("🚀 Jobs page loaded");
    attachAllEventListeners();
    initializeSorting();
    updateFilterOptions(true); // Populate filters based on initial full data
    restoreFilters();
    restoreSortState();
    updateSortIndicators();
    filterJobs(); // Apply filters and initial sort
    restoreExpandedState(); // --- NEW: Restore expanded state on load ---
    initializeLiveUpdateToggle();
    updateLastUpdatedTime();

    // Check for refresh flag
    if (sessionStorage.getItem('jobsWasRefreshed')) {
        dtUtils.showAlert('Data refreshed successfully!', 'success');
        sessionStorage.removeItem('jobsWasRefreshed');
    }
});

function attachAllEventListeners() {
    // Accordion listener
    const accordion = document.querySelector('.jobs-accordion');
    if (accordion) {
        accordion.addEventListener('click', function(event) {
            const header = event.target.closest('.job-header');
            if (header) {
                toggleAccordion(header);
                saveExpandedState(); // --- NEW: Save state on user click ---
            }
        });
    }

    // Filter listeners
    document.getElementById('customerFilter').addEventListener('change', filterJobs);
    document.getElementById('jobFilter').addEventListener('change', filterJobs);
    document.getElementById('partFilter').addEventListener('change', filterJobs);
    document.getElementById('soFilter').addEventListener('change', filterJobs);
    document.getElementById('resetBtn').addEventListener('click', resetFilters);

    // Refresh Button Listener
    const refreshBtn = document.getElementById('refreshBtn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            saveFilters(); // Save current filters
            saveSortState(); // Save current sort
            saveExpandedState(); // --- NEW: Save expanded state ---
            sessionStorage.setItem('jobsWasRefreshed', 'true'); // Set refresh flag
            window.location.reload(); // Reload the page
        });
    }
}

// Update Last Updated Time function
function updateLastUpdatedTime() {
    const timestampEl = document.getElementById('lastUpdated');
    if (timestampEl) {
        const now = new Date();
        const timeString = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        timestampEl.textContent = `Last Updated: ${timeString}`;
    }
}


function initializeSorting() {
    document.querySelectorAll('.job-header-static .sortable').forEach(th => {
        th.addEventListener('click', handleSort);
    });
}

function initializeLiveUpdateToggle() {
    const toggle = document.getElementById('liveUpdateToggle');
    const updateStatus = document.getElementById('updateStatus');
    if (!toggle || !updateStatus) return;
    const isLiveEnabled = localStorage.getItem(LIVE_UPDATE_KEY) === 'true';
    toggle.checked = isLiveEnabled;
    updateStatus.textContent = isLiveEnabled ? 'ON' : 'OFF';
    if (isLiveEnabled) { startLiveUpdate(); }
    toggle.addEventListener('change', function() {
        if (this.checked) {
            localStorage.setItem(LIVE_UPDATE_KEY, 'true');
            updateStatus.textContent = 'ON - Updating...';
            startLiveUpdate();
            fetchAndUpdateData();
        } else {
            localStorage.setItem(LIVE_UPDATE_KEY, 'false');
            updateStatus.textContent = 'OFF';
            stopLiveUpdate();
        }
    });
}

// --- Filter Logic ---

function saveFilters() {
    const filters = {
        customer: document.getElementById('customerFilter').value,
        job: document.getElementById('jobFilter').value,
        part: document.getElementById('partFilter').value,
        so: document.getElementById('soFilter').value,
    };
    sessionStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(filters));
}

function restoreFilters() {
    const savedFilters = JSON.parse(sessionStorage.getItem(FILTER_STORAGE_KEY));
    if (savedFilters) {
        document.getElementById('customerFilter').value = savedFilters.customer || '';
        document.getElementById('jobFilter').value = savedFilters.job || '';
        document.getElementById('partFilter').value = savedFilters.part || '';
        document.getElementById('soFilter').value = savedFilters.so || '';
    }
}

function filterJobs() {
    const customerFilter = document.getElementById('customerFilter').value;
    const jobFilter = document.getElementById('jobFilter').value;
    const partFilter = document.getElementById('partFilter').value;
    const soFilter = document.getElementById('soFilter').value;
    const allJobCards = document.querySelectorAll('.jobs-accordion .job-card');
    let visibleCount = 0;
    allJobCards.forEach(card => {
        const customer = card.dataset.customer || ''; const job = card.dataset.job || '';
        const part = card.dataset.part || ''; const so = card.dataset.so || '';
        let show = true;
        if (customerFilter && customer !== customerFilter) show = false;
        if (jobFilter && job !== jobFilter) show = false;
        if (partFilter && part !== partFilter) show = false;
        if (soFilter && so !== soFilter) show = false;
        card.classList.toggle('hidden-row', !show);
        if (show) { visibleCount++; }
    });
    updateRowCount(visibleCount, allJobCards.length);
    saveFilters();
    updateFilterOptions(false); // Update options based on visible rows
    sortTable(); // Apply sort after filtering
    updateSortIndicators(); // Update indicators after sorting
}

function resetFilters() {
    document.getElementById('customerFilter').value = ''; document.getElementById('jobFilter').value = '';
    document.getElementById('partFilter').value = ''; document.getElementById('soFilter').value = '';
    sessionStorage.removeItem(FILTER_STORAGE_KEY);
    sessionStorage.removeItem(SORT_STORAGE_KEY);
    sessionStorage.removeItem(EXPANDED_JOBS_KEY); // --- NEW: Clear expanded state on reset ---
    restoreSortState(); // Restore default sort
    filterJobs(); // Re-apply empty filters, update options, and sort
}

function updateFilterOptions(isInitialLoad = false) {
    const currentFilters = { customer: document.getElementById('customerFilter').value, job: document.getElementById('jobFilter').value, part: document.getElementById('partFilter').value, so: document.getElementById('soFilter').value, };
    const cardsToScan = isInitialLoad ? document.querySelectorAll('.jobs-accordion .job-card') : document.querySelectorAll('.jobs-accordion .job-card:not(.hidden-row)');
    const optionsMap = { customer: new Set(['']), job: new Set(['']), part: new Set(['']), so: new Set(['']) };
    cardsToScan.forEach(card => {
        const customer = card.dataset.customer || ''; const job = card.dataset.job || ''; const part = card.dataset.part || ''; const so = card.dataset.so || '';
        if (customer && customer !== 'N/A') optionsMap.customer.add(customer);
        if (job) optionsMap.job.add(job); if (part) optionsMap.part.add(part); if (so) optionsMap.so.add(so);
    });
    const sortOptions = (optionsSet) => { return Array.from(optionsSet).sort((a, b) => { if (a === '') return -1; if (b === '') return 1; const numA = parseFloat(a); const numB = parseFloat(b); if (!isNaN(numA) && !isNaN(numB)) { return numA - numB; } return a.localeCompare(b); }); };
    populateSelect('customerFilter', sortOptions(optionsMap.customer), currentFilters.customer);
    populateSelect('jobFilter', sortOptions(optionsMap.job), currentFilters.job);
    populateSelect('partFilter', sortOptions(optionsMap.part), currentFilters.part);
    populateSelect('soFilter', sortOptions(optionsMap.so), currentFilters.so);
}

function populateSelect(selectId, options, selectedValue) {
    const select = document.getElementById(selectId); if (!select) return; select.innerHTML = '';
    options.forEach(optionText => {
        const option = document.createElement('option'); option.value = optionText; option.textContent = optionText || 'All';
        if (optionText === selectedValue) { option.selected = true; } select.appendChild(option);
    });
}

function updateRowCount(visible, total) {
    const rowCountEl = document.getElementById('rowCount'); if (rowCountEl) { rowCountEl.textContent = `Showing ${visible} of ${total} jobs`; }
}

// --- Sorting Functions ---

function saveSortState() {
    sessionStorage.setItem(SORT_STORAGE_KEY, JSON.stringify(sortState));
}

function restoreSortState() {
    const savedSort = sessionStorage.getItem(SORT_STORAGE_KEY); let needsDefault = true;
    if (savedSort) {
        try {
            const parsedSort = JSON.parse(savedSort);
            if (parsedSort && typeof parsedSort === 'object' && 'column' in parsedSort) {
                const headerEl = document.querySelector(`.sortable[data-column-id="${parsedSort.column}"]`);
                if(headerEl) {
                    sortState = { ...parsedSort, columnIndex: Array.from(headerEl.parentElement.children).indexOf(headerEl), columnType: headerEl.dataset.type || 'string' };
                    needsDefault = false;
                } else { sessionStorage.removeItem(SORT_STORAGE_KEY); }
            }
        } catch (e) { console.error("Could not parse saved sort state:", e); sessionStorage.removeItem(SORT_STORAGE_KEY); }
    }
    if (needsDefault) {
        const defaultHeader = document.querySelector('.sortable[data-column-id="job"]');
        if (defaultHeader) { sortState = { column: 'job', direction: 'asc', columnIndex: Array.from(defaultHeader.parentElement.children).indexOf(defaultHeader), columnType: defaultHeader.dataset.type || 'string' }; }
    }
}

function handleSort(e) {
    const th = e.currentTarget; const columnId = th.dataset.columnId; const columnType = th.dataset.type || 'string'; const columnIndex = Array.from(th.parentElement.children).indexOf(th);
    if (sortState.column === columnId) { sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc'; } else { sortState.column = columnId; sortState.direction = 'asc'; }
    sortState.columnIndex = columnIndex; sortState.columnType = columnType;
    sortTable(); updateSortIndicators(); saveSortState();
}

function updateSortIndicators() {
    document.querySelectorAll('.job-header-static .sortable').forEach(th => {
        const indicator = th.querySelector('.sort-indicator'); if (!indicator) return; th.classList.remove('sorted-asc', 'sorted-desc'); indicator.textContent = '';
        if (th.dataset.columnId === sortState.column) { th.classList.add(sortState.direction === 'asc' ? 'sorted-asc' : 'sorted-desc'); indicator.textContent = sortState.direction === 'asc' ? '↑' : '↓'; }
    });
}

function getSortValue(jobCard, type) {
    const infoDiv = jobCard.querySelector(`.job-header > .job-info:nth-child(${sortState.columnIndex + 1})`); if (!infoDiv) return null; const cell = infoDiv.querySelector('strong'); if (!cell) return null;
    const text = cell.textContent.trim(); if (!text || text === 'N/A') return type === 'numeric' ? -Infinity : (type === 'date' ? new Date('2999-12-31') : '');
    switch (type) {
        case 'numeric': return parseFloat(text.replace(/,/g, '')) || 0;
        case 'date': const parts = text.split('/'); if (parts.length === 3) { return new Date(parts[2], parts[0] - 1, parts[1]); } return new Date('2999-12-31');
        default: return text.toLowerCase();
    }
}

function sortTable() {
    if (!sortState.column || sortState.columnIndex < 0) { console.warn("Sort state invalid, skipping sort:", sortState); return; }
    const accordion = document.querySelector('.jobs-accordion');
    const cardsToSort = Array.from(accordion.querySelectorAll('.job-card:not(.hidden-row)'));
    cardsToSort.sort((a, b) => {
        const valA = getSortValue(a, sortState.columnType); const valB = getSortValue(b, sortState.columnType);
        let comparison = 0; if (sortState.columnType === 'string') { comparison = valA.localeCompare(valB); } else { if (valA > valB) comparison = 1; else if (valA < valB) comparison = -1; }
        return sortState.direction === 'asc' ? comparison : -comparison;
    });
    cardsToSort.forEach(card => accordion.appendChild(card)); // Re-append sorted cards
}

// --- Accordion Logic ---
function toggleAccordion(header) {
    header.classList.toggle('expanded');
    const details = header.nextElementSibling;
    if (details && details.classList.contains('transaction-details')) {
        if (details.style.display === 'block') { slideUp(details); }
        else { slideDown(details); }
    }
}

// --- NEW: Expanded State Persistence ---
function saveExpandedState() {
    const expandedJobIds = [];
    document.querySelectorAll('.job-header.expanded').forEach(header => {
        const card = header.closest('.job-card');
        if (card && card.dataset.jobId) {
            expandedJobIds.push(card.dataset.jobId);
        }
    });
    sessionStorage.setItem(EXPANDED_JOBS_KEY, JSON.stringify(expandedJobIds));
    console.log("Saved expanded state:", expandedJobIds);
}

function restoreExpandedState() {
    const expandedJobIds = JSON.parse(sessionStorage.getItem(EXPANDED_JOBS_KEY) || '[]');
    if (expandedJobIds.length === 0) {
        console.log("No expanded state to restore.");
        return;
    }
    console.log("Restoring expanded state:", expandedJobIds);
    
    expandedJobIds.forEach(jobId => {
        const card = document.querySelector(`.job-card[data-job-id="${jobId}"]`);
        if (card) {
            const header = card.querySelector('.job-header');
            const details = card.querySelector('.transaction-details');
            if (header && details && !header.classList.contains('expanded')) {
                header.classList.add('expanded');
                details.style.display = 'block'; // Directly set display for non-animated restore
            }
        }
    });
}


// --- Live Update Logic ---
function startLiveUpdate() {
    if (refreshIntervalId === null) {
        console.log(`Starting live update interval (${REFRESH_INTERVAL_MS / 1000}s)`);
        refreshIntervalId = setInterval(fetchAndUpdateData, REFRESH_INTERVAL_MS);
        document.getElementById('updateStatus').textContent = 'ON';
    }
}

function stopLiveUpdate() {
    if (refreshIntervalId !== null) {
        console.log("Stopping live update interval");
        clearInterval(refreshIntervalId);
        refreshIntervalId = null;
        document.getElementById('updateStatus').textContent = 'OFF';
    }
}

function fetchAndUpdateData() {
    console.log("Fetching live job data...");
    const updateStatus = document.getElementById('updateStatus'); if (updateStatus) updateStatus.textContent = 'ON - Updating...';
    
    // Store current state *before* fetching
    const currentFiltersBeforeFetch = { customer: document.getElementById('customerFilter').value, job: document.getElementById('jobFilter').value, part: document.getElementById('partFilter').value, so: document.getElementById('soFilter').value, };
    const currentSortBeforeFetch = { ...sortState };
    saveExpandedState(); // --- NEW: Save expanded state before fetch ---

    fetch('/jobs/api/open-jobs-data')
        .then(response => { if (!response.ok) throw new Error(`HTTP error ${response.status}`); return response.json(); })
        .then(data => {
            if (data.success) {
                updateTableData(data.jobs); // Update DOM
                updateLastUpdatedTime(); // Update timestamp
                updateFilterOptions(true); // Repopulate ALL options first

                // Restore Filter Selections
                Object.keys(currentFiltersBeforeFetch).forEach(key => {
                    const selectId = key + 'Filter'; const selectElement = document.getElementById(selectId);
                    if (selectElement) { const previousValue = currentFiltersBeforeFetch[key]; const exists = Array.from(selectElement.options).some(opt => opt.value === previousValue); selectElement.value = exists ? previousValue : ''; }
                });

                // Restore Sort State
                sortState = currentSortBeforeFetch;

                // Refilter and Resort view
                filterJobs(); 
                
                // --- NEW: Restore expanded state *after* filtering/sorting ---
                restoreExpandedState();

                if (updateStatus) updateStatus.textContent = 'ON';
            } else { throw new Error(data.message || 'API Error'); }
        })
        .catch(error => {
            console.error('Error fetching live data:', error); dtUtils.showAlert(`Failed to fetch live data: ${error.message}`, 'error'); if (updateStatus) updateStatus.textContent = 'ON - Error';
            stopLiveUpdate(); const toggle = document.getElementById('liveUpdateToggle'); if(toggle) toggle.checked = false; localStorage.setItem(LIVE_UPDATE_KEY, 'false');
        });
}


function updateTableData(newJobsData) {
    console.log("Updating table data...");
    const accordion = document.querySelector('.jobs-accordion');
    const existingJobCardsMap = new Map();
    
    // --- MODIFIED: We only need to know existing IDs ---
    accordion.querySelectorAll('.job-card').forEach(card => {
        existingJobCardsMap.set(card.dataset.jobId, card);
    });
    const processedJobIds = new Set();

    newJobsData.forEach(newJob => {
        const jobId = newJob.job_number;
        processedJobIds.add(jobId);
        let jobCard = existingJobCardsMap.get(jobId);

        if (!jobCard) {
            jobCard = createJobCardElement(newJob);
            accordion.appendChild(jobCard);
        } else {
            // Update existing card
            jobCard.dataset.customer = newJob.customer_name || '';
            jobCard.dataset.job = newJob.job_number || '';
            jobCard.dataset.part = newJob.part_number || '';
            jobCard.dataset.so = newJob.sales_order || '';
            // Update Header Content
            updateCell(jobCard.querySelector('[data-field="job_number"]'), newJob.job_number);
            updateCell(jobCard.querySelector('[data-field="part_number"]'), newJob.part_number);
            updateCell(jobCard.querySelector('[data-field="customer_name"]'), newJob.customer_name);
            updateCell(jobCard.querySelector('[data-field="sales_order"]'), newJob.sales_order);
            updateCell(jobCard.querySelector('[data-field="required_qty"]'), parseFloat(newJob.required_qty || 0), true);
            updateCell(jobCard.querySelector('[data-field="completed_qty"] strong'), parseFloat(newJob.completed_qty || 0), true);
            const completedQtyHeaderCell = jobCard.querySelector('.job-info[data-field="completed_qty"]');
            if (completedQtyHeaderCell) completedQtyHeaderCell.title = newJob.tooltip || '';
            
            // Update Details Table (No change to this function call)
            updateDetailsTable(jobCard, newJob.aggregated_list);
        }
    });
    // Remove Old Job Cards
    existingJobCardsMap.forEach((card, jobId) => {
        if (!processedJobIds.has(jobId)) {
            card.remove();
            console.log(`Removed job card: ${jobId}`);
        }
    });
    
    console.log("Table update complete.");
}

function createJobCardElement(job) {
     const card = document.createElement('div'); card.className = 'job-card'; card.dataset.jobId = job.job_number; card.dataset.customer = job.customer_name || ''; card.dataset.job = job.job_number || ''; card.dataset.part = job.part_number || ''; card.dataset.so = job.sales_order || '';
     const header = document.createElement('div'); header.className = 'job-header';
     // Add click listener directly here
     header.addEventListener('click', () => {
         toggleAccordion(header);
         saveExpandedState(); // --- NEW: Save state on user click ---
     });
     header.innerHTML = `
         <div class="job-info" data-column-id="job"><strong data-field="job_number">${job.job_number}</strong></div>
         <div class="job-info" data-column-id="part"><strong data-field="part_number">${job.part_number}</strong></div>
         <div class="job-info" data-column-id="customer"><strong data-field="customer_name">${job.customer_name}</strong></div>
         <div class="job-info" data-column-id="so"><strong data-field="sales_order">${job.sales_order}</strong></div>
         <div class="job-info numeric" data-column-id="required"><strong data-field="required_qty">${(parseFloat(job.required_qty || 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</strong></div>
         <div class="job-info numeric" data-column-id="completed" data-field="completed_qty" title="${job.tooltip || ''}"><strong>${(parseFloat(job.completed_qty || 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</strong></div>
         <div class="expand-icon">›</div>`;
     card.appendChild(header);
     const details = document.createElement('div'); details.className = 'transaction-details';
     details.innerHTML = `<table class="transaction-table"><thead><tr><th>Part</th><th>Part Description</th><th class="numeric">Issued Inventory</th><th class="numeric">De-issue</th><th class="numeric">Relieve Job</th><th class="numeric">Yield Cost/Scrap</th><th class="numeric">Yield Loss</th></tr></thead><tbody></tbody></table>`;
     updateDetailsTable(card, job.aggregated_list, details); card.appendChild(details); return card;
}

function updateDetailsTable(jobCard, aggregatedList, detailsElement = null) {
    const tbody = detailsElement ? detailsElement.querySelector('tbody') : jobCard.querySelector('.transaction-table tbody'); if (!tbody) return;
    const existingRowsMap = new Map(); tbody.querySelectorAll('tr').forEach(row => existingRowsMap.set(row.dataset.partNumber, row)); const processedParts = new Set();
    
    // Add new/update existing rows
    aggregatedList.forEach(partSummary => {
        const partNumber = partSummary.part_number; processedParts.add(partNumber); let partRow = existingRowsMap.get(partNumber);
        if (!partRow) { partRow = document.createElement('tr'); partRow.dataset.partNumber = partNumber; tbody.appendChild(partRow); } // Append new row
        
        // Update content
        partRow.innerHTML = `
            <td>${partNumber}</td><td>${partSummary.part_description || ''}</td>
            <td class="numeric" data-field="Issued inventory">${(parseFloat(partSummary['Issued inventory'] || 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            <td class="numeric" data-field="De-issue">${(parseFloat(partSummary['De-issue'] || 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            <td class="numeric" data-field="Relieve Job">${(parseFloat(partSummary['Relieve Job'] || 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            <td class="numeric" data-field="Yield Cost/Scrap">${(parseFloat(partSummary['Yield Cost/Scrap'] || 0)).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            <td class="numeric" data-field="Yield Loss">${(parseFloat(partSummary['Yield Loss'] || 0)).toFixed(2)}%</td>
        `;
    });
    // Remove old rows
    existingRowsMap.forEach((row, partNumber) => { if (!processedParts.has(partNumber)) row.remove(); });

    // Add empty state message if tbody is empty
    if(tbody.children.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-tertiary);">No component transactions found.</td></tr>`;
    }
}


function updateCell(cellElement, newValue, formatAsNumber = false, suffix = '') {
    if (!cellElement) return; let formattedNewValue;
    if (newValue === null || newValue === undefined) { formattedNewValue = (suffix === '%') ? '0.00%' : (formatAsNumber ? '0.00' : 'N/A'); }
    else if (typeof newValue === 'number') {
        if (formatAsNumber) { formattedNewValue = newValue.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
        else if (suffix === '%') { formattedNewValue = newValue.toFixed(2) + '%'; }
        else { formattedNewValue = newValue.toString(); }
    } else { formattedNewValue = newValue.toString(); }
    
    const currentFormattedValue = cellElement.textContent.trim();
    if (currentFormattedValue !== formattedNewValue) {
        cellElement.textContent = formattedNewValue;
        const parentEl = cellElement.closest('.job-info') || cellElement.closest('td');
        if (parentEl) {
             parentEl.classList.add('cell-changed');
             setTimeout(() => { parentEl.classList.remove('cell-changed'); }, 1500);
        }
    }
}

// Slide functions
function slideDown(element) {
    element.style.display = 'block'; let height = element.scrollHeight + 'px'; element.style.height = 0;
    setTimeout(() => { element.style.transition = 'height 0.3s ease-in-out'; element.style.height = height; }, 0);
    setTimeout(() => { element.style.removeProperty('height'); element.style.removeProperty('transition'); }, 300);
}
function slideUp(element) {
    element.style.height = element.scrollHeight + 'px';
    setTimeout(() => { element.style.transition = 'height 0.3s ease-in-out'; element.style.height = 0; }, 0);
    setTimeout(() => { element.style.display = 'none'; element.style.removeProperty('height'); element.style.removeProperty('transition'); }, 300);
}

// Utility function placeholder
const dtUtils = window.dtUtils || {
    showAlert: (message, type) => {
        const alertsDiv = document.getElementById('alerts');
        if (!alertsDiv) { console.log(`Alert (${type}): ${message}`); return; } // Fallback
        const alertClass = type === 'success' ? 'alert-success' : type === 'error' ? 'alert-error' : 'alert-info';
        const alertElement = document.createElement('div');
        alertElement.className = `alert ${alertClass}`;
        alertElement.textContent = message;
        alertsDiv.appendChild(alertElement);
        setTimeout(() => {
            alertElement.style.opacity = '0';
            setTimeout(() => {
                if (alertElement.parentElement) {
                    alertElement.parentElement.removeChild(alertElement);
                }
            }, 500); // Match CSS transition
        }, 5000);
    }
};