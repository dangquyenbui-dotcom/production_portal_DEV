document.addEventListener('DOMContentLoaded', function() {
    
    // --- INITIALIZATION ---
    attachAllEventListeners();
    
    // 1. Populate filters with all possible options from the full dataset first.
    updateFilterOptions(); 
    
    // 2. Now that all <option> elements exist, restore the saved selections.
    restoreFilters();      
    
    // 3. Restore the previously saved sort state (or set a default).
    restoreSortState();
    
    // 4. NEW: Update the visual sort indicators to match the restored state.
    updateSortIndicators();
    
    // 5. Finally, run the filter to update the grid view and apply the sort.
    filterMRP();          
    
    updateLastUpdatedTime();

    // Check for refresh flag
    if (sessionStorage.getItem('mrpWasRefreshed')) {
        dtUtils.showAlert('Data refreshed successfully!', 'success');
        sessionStorage.removeItem('mrpWasRefreshed');
    }
});

let sortState = {
    column: null,
    direction: 'none', // 'asc', 'desc'
    columnIndex: -1,
    columnType: 'string'
};

function attachAllEventListeners() {
    attachAccordionEventListeners();

    // Filter changes
    document.getElementById('buFilter').addEventListener('change', filterMRP);
    document.getElementById('customerFilter').addEventListener('change', filterMRP);
    document.getElementById('fgFilter').addEventListener('change', filterMRP);
    document.getElementById('dueShipFilter').addEventListener('change', filterMRP);
    document.getElementById('statusFilter').addEventListener('change', filterMRP);
    document.getElementById('resetBtn').addEventListener('click', resetFilters);
    document.getElementById('exportBtn').addEventListener('click', exportVisibleDataToXlsx);
    document.getElementById('refreshBtn').addEventListener('click', () => {
        saveFilters();
        saveSortState(); // Save sort state before reloading
        sessionStorage.setItem('mrpWasRefreshed', 'true');
        window.location.reload();
    });
    
    document.querySelectorAll('.so-header-static .sortable').forEach(header => {
        header.addEventListener('click', handleSortClick);
    });
}

function attachAccordionEventListeners() {
    const accordion = document.querySelector('.mrp-accordion');
    if (!accordion) return;

    accordion.addEventListener('click', function(event) {
        const header = event.target.closest('.so-header:not(.no-expand)');
        if (header) {
            header.classList.toggle('expanded');
            const details = document.getElementById(header.dataset.target);
            if (details) {
                if (details.style.display === 'block') {
                    slideUp(details);
                } else {
                    slideDown(details);
                }
            }
        }
    });
}

function saveSortState() {
    sessionStorage.setItem('mrpSortState', JSON.stringify(sortState));
}

function restoreSortState() {
    const savedSort = sessionStorage.getItem('mrpSortState');
    if (savedSort) {
        try {
            const parsedSort = JSON.parse(savedSort);
            if (parsedSort && typeof parsedSort === 'object' && 'column' in parsedSort) {
                sortState = parsedSort;
                return; 
            }
        } catch (e) {
            console.error("Could not parse saved sort state:", e);
            sessionStorage.removeItem('mrpSortState'); // Clear invalid data
        }
    }

    // If nothing is saved or parsing fails, set a default sort by Sales Order, ascending.
    const soHeader = document.querySelector('.sortable[data-column-id="SO"]');
    if (soHeader) {
        const columnIndex = Array.from(soHeader.parentElement.children).indexOf(soHeader);
        sortState = {
            column: 'SO',
            direction: 'asc',
            columnIndex: columnIndex,
            columnType: 'string'
        };
    }
}


function updateLastUpdatedTime() {
    const timestampEl = document.getElementById('lastUpdated');
    if (timestampEl) {
        const now = new Date();
        const timeString = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        timestampEl.textContent = `Last Updated: ${timeString}`;
    }
}

function updateFilterOptions() {
    const selectedBU = document.getElementById('buFilter').value;
    const selectedCustomer = document.getElementById('customerFilter').value;
    const selectedFG = document.getElementById('fgFilter').value;
    const selectedDueShip = document.getElementById('dueShipFilter').value;
    const selectedStatus = document.getElementById('statusFilter').value;

    const allRows = document.querySelectorAll('.mrp-accordion .so-header');

    const getOptionsFor = (filterToUpdate) => {
        const options = new Set();
        let hasBlank = false;

        allRows.forEach(row => {
            const bu = row.dataset.bu;
            const customer = row.dataset.customer;
            const fg = row.dataset.fg;
            const dueDate = row.dataset.dueShip || '';
            const status = row.dataset.status;
            const dueDateMonthYear = (dueDate && dueDate.includes('/')) ? `${dueDate.split('/')[0].padStart(2, '0')}/${dueDate.split('/')[2]}` : 'Blank';

            let matches = true;
            if (filterToUpdate !== 'bu' && selectedBU && bu !== selectedBU) matches = false;
            if (filterToUpdate !== 'customer' && selectedCustomer && customer !== selectedCustomer) matches = false;
            if (filterToUpdate !== 'fg' && selectedFG && fg !== selectedFG) matches = false;
            if (filterToUpdate !== 'status' && selectedStatus) {
                let matchesStatus = false;
                const productionNeededStatuses = ['ok', 'partial', 'partial-ship', 'job-created'];
                const actionRequiredStatuses = ['critical', 'pending-qc'];
                if (selectedStatus === 'ready-to-ship') {
                    if (status === 'ready-to-ship') matchesStatus = true;
                } else if (selectedStatus === 'production-needed') {
                    if (productionNeededStatuses.includes(status)) matchesStatus = true;
                } else if (selectedStatus === 'action-required') {
                    if (actionRequiredStatuses.includes(status)) matchesStatus = true;
                }
                if (!matchesStatus) matches = false;
            }
            if (filterToUpdate !== 'dueShip' && selectedDueShip) {
                 if (selectedDueShip === 'Blank' && dueDate !== '') matches = false;
                 else if (selectedDueShip !== 'Blank' && dueDateMonthYear !== selectedDueShip) matches = false;
            }

            if (matches) {
                switch(filterToUpdate) {
                    case 'bu': options.add(bu); break;
                    case 'customer': options.add(customer); break;
                    case 'fg': options.add(fg); break;
                    case 'dueShip':
                        if (dueDateMonthYear === 'Blank') hasBlank = true;
                        else options.add(dueDateMonthYear);
                        break;
                }
            }
        });
        return { options: [...options].sort(), hasBlank };
    };

    const buOpts = getOptionsFor('bu');
    const customerOpts = getOptionsFor('customer');
    const fgOpts = getOptionsFor('fg');
    const dueDateOpts = getOptionsFor('dueShip');

    const sortedDueDates = dueDateOpts.options.sort((a, b) => {
        if (a === 'Blank') return 1; if (b === 'Blank') return -1;
        const [aMonth, aYear] = a.split('/');
        const [bMonth, bYear] = b.split('/');
        return new Date(aYear, aMonth - 1) - new Date(bYear, bMonth - 1);
    });

    populateSelect('buFilter', buOpts.options, false, selectedBU);
    populateSelect('customerFilter', customerOpts.options, false, selectedCustomer);
    populateSelect('fgFilter', fgOpts.options, false, selectedFG);
    populateSelect('dueShipFilter', sortedDueDates, dueDateOpts.hasBlank, selectedDueShip);
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
        blankOption.textContent = '(No Date)';
        select.appendChild(blankOption);
    }
    
    if (selectedValue) {
        const optionExists = Array.from(select.options).some(opt => opt.value === selectedValue);
        if (optionExists) {
            select.value = selectedValue;
        }
    }
}

function saveFilters() {
    const filters = {
        bu: document.getElementById('buFilter').value,
        customer: document.getElementById('customerFilter').value,
        fg: document.getElementById('fgFilter').value,
        dueShip: document.getElementById('dueShipFilter').value,
        status: document.getElementById('statusFilter').value,
    };
    sessionStorage.setItem('mrpFilters', JSON.stringify(filters));
}

function restoreFilters() {
    const savedFilters = JSON.parse(sessionStorage.getItem('mrpFilters'));
    if (savedFilters) {
        document.getElementById('buFilter').value = savedFilters.bu || '';
        document.getElementById('customerFilter').value = savedFilters.customer || '';
        document.getElementById('fgFilter').value = savedFilters.fg || '';
        document.getElementById('dueShipFilter').value = savedFilters.dueShip || '';
        document.getElementById('statusFilter').value = savedFilters.status || '';
    }
}

function filterMRP() {
    const buFilter = document.getElementById('buFilter').value;
    const customerFilter = document.getElementById('customerFilter').value;
    const fgFilter = document.getElementById('fgFilter').value;
    const dueShipFilter = document.getElementById('dueShipFilter').value;
    const statusFilter = document.getElementById('statusFilter').value;

    let visibleCount = 0;
    let okCount = 0, partialCount = 0, criticalCount = 0, readyToShipCount = 0;
    let pendingQCCount = 0, jobCreatedCount = 0, partialShipmentCount = 0;

    const canProduceHeader = document.querySelector('.so-header-static [data-column-id="CanProduce"] label');
    if (canProduceHeader) {
        switch (statusFilter) {
            case 'ready-to-ship': canProduceHeader.textContent = 'Shippable Qty'; break;
            case 'job-created': canProduceHeader.textContent = 'Shippable On-Hand'; break;
            default: canProduceHeader.textContent = 'Deliverable Qty'; break;
        }
    }

    document.querySelectorAll('.so-header').forEach(header => {
        let show = true;
        const status = header.dataset.status;
        const dueDate = header.dataset.dueShip || '';
        const dueDateMonthYear = (dueDate && dueDate.includes('/')) ? `${dueDate.split('/')[0].padStart(2, '0')}/${dueDate.split('/')[2]}` : 'Blank';

        if (buFilter && header.dataset.bu !== buFilter) show = false;
        if (customerFilter && header.dataset.customer !== customerFilter) show = false;
        if (fgFilter && header.dataset.fg !== fgFilter) show = false;
        if (dueShipFilter) {
            if (dueShipFilter === 'Blank' && dueDate !== '') show = false;
            else if (dueShipFilter !== 'Blank' && dueDateMonthYear !== dueShipFilter) show = false;
        }
        
        if (statusFilter) {
            let matchesStatus = false;
            const productionNeededStatuses = ['ok', 'partial', 'partial-ship', 'job-created'];
            const actionRequiredStatuses = ['critical', 'pending-qc'];

            if (statusFilter === 'ready-to-ship') {
                if (status === 'ready-to-ship') matchesStatus = true;
            } else if (statusFilter === 'production-needed') {
                if (productionNeededStatuses.includes(status)) matchesStatus = true;
            } else if (statusFilter === 'action-required') {
                if (actionRequiredStatuses.includes(status)) matchesStatus = true;
            }
            if (!matchesStatus) show = false;
        }

        header.classList.toggle('hidden-row', !show);
        
        if(show) {
            visibleCount++;
            switch(status) {
                case 'ready-to-ship': readyToShipCount++; break;
                case 'pending-qc': pendingQCCount++; break;
                case 'job-created': jobCreatedCount++; break;
                case 'ok': okCount++; break;
                case 'partial-ship': partialShipmentCount++; break;
                case 'partial': partialCount++; break;
                case 'critical': criticalCount++; break;
            }
        }
    });
    
    updateSummaryCards(visibleCount, readyToShipCount, pendingQCCount, okCount, partialCount, criticalCount, jobCreatedCount, partialShipmentCount);
    updateRowCount();
    saveFilters();
    updateFilterOptions();
    sortMRP();
    updateSortIndicators(); // Update indicators after sorting
}

function updateRowCount() {
    const totalRows = document.querySelectorAll('.mrp-accordion .so-header').length;
    const visibleRows = document.querySelectorAll('.mrp-accordion .so-header:not(.hidden-row)').length;
    const rowCountEl = document.getElementById('rowCount');
    if (rowCountEl) {
        if (totalRows > 0) {
            rowCountEl.textContent = `Showing ${visibleRows} of ${totalRows} rows`;
        } else {
            rowCountEl.textContent = 'No rows to display';
        }
    }
}

function resetFilters() {
    document.getElementById('buFilter').value = '';
    document.getElementById('customerFilter').value = '';
    document.getElementById('fgFilter').value = '';
    document.getElementById('dueShipFilter').value = '';
    document.getElementById('statusFilter').value = '';
    sessionStorage.removeItem('mrpFilters');
    sessionStorage.removeItem('mrpSortState'); // Also clear sort on reset
    location.reload(); // Reload to apply default sort and filters
}

function updateSummaryCards(total, readyToShip, pendingQC, ok, partial, critical, jobCreated, partialShipment) {
    document.getElementById('total-orders').textContent = total;
    document.getElementById('ready-to-ship-count').textContent = readyToShip;
    document.getElementById('pending-qc-count').textContent = pendingQC;
    document.getElementById('full-production').textContent = ok;
    document.getElementById('partial-production').textContent = partial;
    document.getElementById('critical-shortage').textContent = critical;
    document.getElementById('job-created-count').textContent = jobCreated;
    document.getElementById('partial-shipment-count').textContent = partialShipment;
}

function handleSortClick(e) {
    const header = e.currentTarget;
    const columnId = header.dataset.columnId;

    if (sortState.column === columnId) {
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.column = columnId;
        sortState.direction = 'asc';
    }
    sortState.columnIndex = Array.from(header.parentElement.children).indexOf(header);
    sortState.columnType = header.dataset.columnType;
    
    sortMRP();
    updateSortIndicators();
}

function updateSortIndicators() {
    document.querySelectorAll('.so-header-static .sortable').forEach(header => {
        const indicator = header.querySelector('.sort-indicator');
        header.classList.remove('sorted-asc', 'sorted-desc');
        indicator.textContent = '';
        if (header.dataset.columnId === sortState.column) {
            if (sortState.direction === 'asc') {
                header.classList.add('sorted-asc');
                indicator.textContent = '↑';
            } else {
                header.classList.add('sorted-desc');
                indicator.textContent = '↓';
            }
        }
    });
}

function sortMRP() {
    if (!sortState.column) return;
    const accordion = document.querySelector('.mrp-accordion');
    const orderGroups = Array.from(accordion.querySelectorAll('.so-header')).map(header => {
        const details = document.getElementById(header.dataset.target);
        return { header, details: details || null };
    });

    const getSortValue = (header) => {
        if (sortState.columnIndex < 0) return null;
        const infoDiv = header.children[sortState.columnIndex];
        const text = infoDiv ? infoDiv.querySelector('strong, div').textContent.trim() : '';
        
        if (sortState.columnType === 'date') {
            if (!text || text.toLowerCase() === 'n/a') return new Date('2999-12-31'); // Push empty dates to the end
            const parts = text.split('/');
            return parts.length === 3 ? new Date(parts[2], parts[0] - 1, parts[1]) : new Date('2999-12-31');
        }
        
        return sortState.columnType === 'numeric' ? (parseFloat(text.replace(/,/g, '')) || 0) : text.toLowerCase();
    };

    orderGroups.sort((a, b) => {
        const valA = getSortValue(a.header);
        const valB = getSortValue(b.header);
        let comparison = 0;
        if (valA > valB) comparison = 1;
        else if (valA < valB) comparison = -1;
        return sortState.direction === 'asc' ? comparison : -comparison;
    });

    orderGroups.forEach(group => {
        accordion.appendChild(group.header);
        if (group.details) {
            accordion.appendChild(group.details);
        }
    });
}

function exportVisibleDataToXlsx() {
    const exportBtn = document.getElementById('exportBtn');
    exportBtn.disabled = true;
    exportBtn.textContent = '📥 Generating...';

    const headers = [
        'SO', 'Customer', 'Finished Good', 'SO Required', 'SO Can Produce', 'SO Bottleneck',
        'Component Part', 'Component Description', 'Total Required', 'Initial On-Hand',
        'Avail. Before SO', 'Allocated', 'Open PO Qty', 'Shortfall'
    ];

    const rows = [];
    document.querySelectorAll('.mrp-accordion .so-header').forEach(header => {
        if (header.classList.contains('hidden-row')) {
            return;
        }

        const soData = {
            so: header.querySelector('.so-info:nth-child(1) strong').textContent.trim(),
            customer: header.querySelector('.so-info:nth-child(2) strong').textContent.trim(),
            fg: header.querySelector('.so-info:nth-child(3) strong').textContent.trim(),
            required: header.querySelector('.so-info:nth-child(5) strong').textContent.trim(),
            canProduce: header.querySelector('.so-info:nth-child(6) strong').textContent.trim(),
            bottleneck: header.querySelector('.so-info:nth-child(7) div').getAttribute('title') || header.querySelector('.so-info:nth-child(7) div').textContent.trim(),
        };

        const detailsId = header.dataset.target;
        if (detailsId && !header.classList.contains('no-expand')) {
            const detailsTable = document.getElementById(detailsId);
            if (detailsTable) {
                detailsTable.querySelectorAll('tbody tr').forEach(compRow => {
                    const rowData = [
                        soData.so, soData.customer, soData.fg, soData.required, soData.canProduce, soData.bottleneck,
                        compRow.cells[0].textContent.trim().replace('🔗', '').trim(),
                        compRow.cells[1].textContent.trim(), compRow.cells[2].textContent.trim(),
                        compRow.cells[3].textContent.trim(), compRow.cells[4].textContent.trim(),
                        compRow.cells[5].textContent.trim(), compRow.cells[6].textContent.trim(),
                        compRow.cells[7].textContent.trim(),
                    ];
                    rows.push(rowData);
                });
            }
        } else {
             rows.push([soData.so, soData.customer, soData.fg, soData.required, soData.canProduce, soData.bottleneck, '', '', '', '', '', '', '', '']);
        }
    });

    if (rows.length === 0) {
        dtUtils.showAlert('No data to export.', 'info');
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
        return;
    }
    
    fetch('/mrp/api/export-xlsx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ headers, rows })
    })
    .then(response => {
        if (!response.ok) { throw new Error('Network response was not ok.'); }
        const disposition = response.headers.get('Content-Disposition');
        const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(disposition);
        const filename = (matches != null && matches[1]) ? matches[1].replace(/['"]/g, '') : 'mrp_export.xlsx';
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

/* Simple slide-down/up animations */
function slideDown(element) {
    element.style.display = 'block';
    let height = element.scrollHeight + 'px';
    element.style.height = 0;
    setTimeout(() => {
        element.style.transition = 'height 0.3s ease-in-out';
        element.style.height = height;
    }, 0);
    setTimeout(() => {
        element.style.removeProperty('height');
        element.style.removeProperty('transition');
    }, 300);
}

function slideUp(element) {
    element.style.height = element.scrollHeight + 'px';
    setTimeout(() => {
        element.style.transition = 'height 0.3s ease-in-out';
        element.style.height = 0;
    }, 0);
    setTimeout(() => {
        element.style.display = 'none';
        element.style.removeProperty('height');
        element.style.removeProperty('transition');
    }, 300);
}