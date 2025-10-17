document.addEventListener('DOMContentLoaded', function() {
    
    // --- INITIALIZATION ---
    attachAllEventListeners();
    updateFilterOptions(); 
    restoreFilters();      
    filterOrders();          
});

function attachAllEventListeners() {
    const accordion = document.querySelector('.mrp-accordion');
    if (accordion) {
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

    // Filter changes
    document.getElementById('buFilter')?.addEventListener('change', filterOrders);
    document.getElementById('fgFilter')?.addEventListener('change', filterOrders);
    document.getElementById('dueShipFilter')?.addEventListener('change', filterOrders);
    document.getElementById('statusFilter')?.addEventListener('change', filterOrders);
    document.getElementById('resetBtn')?.addEventListener('click', resetFilters);
}

function updateFilterOptions() {
    const selectedBU = document.getElementById('buFilter')?.value;
    const selectedFG = document.getElementById('fgFilter')?.value;
    const selectedDueShip = document.getElementById('dueShipFilter')?.value;
    const selectedStatus = document.getElementById('statusFilter')?.value;

    const allCards = document.querySelectorAll('.mrp-accordion .so-card');

    const getOptionsFor = (filterToUpdate) => {
        const options = new Set();
        let hasBlank = false;

        allCards.forEach(card => {
            const bu = card.dataset.bu;
            const fg = card.dataset.fg;
            const dueDate = card.dataset.dueShip || '';
            const status = card.dataset.status;
            const dueDateMonthYear = (dueDate && dueDate.includes('/')) ? `${dueDate.split('/')[0].padStart(2, '0')}/${dueDate.split('/')[2]}` : 'Blank';

            let matches = true;
            if (filterToUpdate !== 'bu' && selectedBU && bu !== selectedBU) matches = false;
            if (filterToUpdate !== 'fg' && selectedFG && fg !== selectedFG) matches = false;
            if (filterToUpdate !== 'status' && selectedStatus && status !== selectedStatus) matches = false;
            if (filterToUpdate !== 'dueShip' && selectedDueShip) {
                 if (selectedDueShip === 'Blank' && dueDate !== '') matches = false;
                 else if (selectedDueShip !== 'Blank' && dueDateMonthYear !== selectedDueShip) matches = false;
            }

            if (matches) {
                switch(filterToUpdate) {
                    case 'bu': options.add(bu); break;
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
    const fgOpts = getOptionsFor('fg');
    const dueDateOpts = getOptionsFor('dueShip');

    const sortedDueDates = dueDateOpts.options.sort((a, b) => {
        if (a === 'Blank') return 1; if (b === 'Blank') return -1;
        const [aMonth, aYear] = a.split('/');
        const [bMonth, bYear] = b.split('/');
        return new Date(aYear, aMonth - 1) - new Date(bYear, bMonth - 1);
    });

    populateSelect('buFilter', buOpts.options, false, selectedBU);
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
        bu: document.getElementById('buFilter')?.value,
        fg: document.getElementById('fgFilter')?.value,
        dueShip: document.getElementById('dueShipFilter')?.value,
        status: document.getElementById('statusFilter')?.value,
    };
    sessionStorage.setItem('mrpSummaryFilters', JSON.stringify(filters));
}

function restoreFilters() {
    const savedFilters = JSON.parse(sessionStorage.getItem('mrpSummaryFilters'));
    if (savedFilters) {
        if (document.getElementById('buFilter')) document.getElementById('buFilter').value = savedFilters.bu || '';
        if (document.getElementById('fgFilter')) document.getElementById('fgFilter').value = savedFilters.fg || '';
        if (document.getElementById('dueShipFilter')) document.getElementById('dueShipFilter').value = savedFilters.dueShip || '';
        if (document.getElementById('statusFilter')) document.getElementById('statusFilter').value = savedFilters.status || '';
    }
}

function filterOrders() {
    const buFilter = document.getElementById('buFilter')?.value;
    const fgFilter = document.getElementById('fgFilter')?.value;
    const dueShipFilter = document.getElementById('dueShipFilter')?.value;
    const statusFilter = document.getElementById('statusFilter')?.value;

    let totalCount = 0, onTrackCount = 0, atRiskCount = 0, criticalCount = 0;

    document.querySelectorAll('.so-card').forEach(card => {
        const status = card.dataset.status;
        const dueDate = card.dataset.dueShip || '';
        const dueDateMonthYear = (dueDate && dueDate.includes('/')) ? `${dueDate.split('/')[0].padStart(2, '0')}/${dueDate.split('/')[2]}` : 'Blank';

        let show = true;
        if (buFilter && card.dataset.bu !== buFilter) show = false;
        if (fgFilter && card.dataset.fg !== fgFilter) show = false;
        if (statusFilter && status !== statusFilter) show = false;
        if (dueShipFilter) {
            if (dueShipFilter === 'Blank' && dueDate !== '') show = false;
            else if (dueShipFilter !== 'Blank' && dueDateMonthYear !== dueShipFilter) show = false;
        }

        card.classList.toggle('hidden-row', !show);
        
        if (show) {
            totalCount++;
            if (status === 'On-Track') onTrackCount++;
            else if (status === 'At-Risk') atRiskCount++;
            else if (status === 'Critical') criticalCount++;
        }
    });
    
    updateSummaryCards(totalCount, onTrackCount, atRiskCount, criticalCount);
    saveFilters();
    updateFilterOptions();
}

function resetFilters() {
    if(document.getElementById('buFilter')) document.getElementById('buFilter').value = '';
    if(document.getElementById('fgFilter')) document.getElementById('fgFilter').value = '';
    if(document.getElementById('dueShipFilter')) document.getElementById('dueShipFilter').value = '';
    if(document.getElementById('statusFilter')) document.getElementById('statusFilter').value = '';
    
    sessionStorage.removeItem('mrpSummaryFilters');
    filterOrders();
}

function updateSummaryCards(total, onTrack, atRisk, critical) {
    document.getElementById('total_orders').textContent = total;
    document.getElementById('on_track_orders').textContent = onTrack;
    document.getElementById('at_risk_orders').textContent = atRisk;
    document.getElementById('critical_orders').textContent = critical;
}

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