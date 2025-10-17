// ============================================
// DOWNTIME ENTRY FORM - COMPLETE JAVASCRIPT
// ============================================

console.log('✅ Downtime entry form loading...');

// Global variables are now passed from the template
// let availableJobs = [];
// let currentLineId = null;
// let allCategories = [];
// const translations = {};

// ============================================
// INITIALIZATION
// ============================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 DOM ready, initializing form...');
    
    initializeDateTimeFields();
    attachEventListeners();
    updateDateTime();
    setInterval(updateDateTime, 1000);
    
    console.log('✅ Form initialized');
});

// ============================================
// DATE/TIME FUNCTIONS
// ============================================

function updateDateTime() {
    const now = new Date();
    const options = { 
        month: 'short', 
        day: 'numeric', 
        hour: '2-digit', 
        minute: '2-digit',
        hour12: true 
    };
    // The locale is now passed from the template
    document.getElementById('current-datetime').textContent = 
        now.toLocaleDateString(pageLocale, options).replace(',', ' ');
}

function initializeDateTimeFields() {
    const now = new Date();
    const thirtyMinutesAgo = new Date(now.getTime() - 30 * 60 * 1000);
    
    document.getElementById('start_time').value = formatDateTimeLocal(thirtyMinutesAgo);
    document.getElementById('end_time').value = formatDateTimeLocal(now);
    
    updateDuration();
}

function formatDateTimeLocal(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function updateDuration() {
    const start = new Date(document.getElementById('start_time').value);
    const end = new Date(document.getElementById('end_time').value);
    
    if (start && end && end > start) {
        const minutes = Math.round((end - start) / 60000);
        document.getElementById('duration-value').textContent = `${minutes} ${translations.minutes}`;
        document.getElementById('duration-display').style.display = 'flex';
    } else {
        document.getElementById('duration-display').style.display = 'none';
    }
}

// ============================================
// EVENT LISTENERS
// ============================================

function attachEventListeners() {
    document.getElementById('facility_id').addEventListener('change', handleFacilityChange);
    document.getElementById('line_id').addEventListener('change', handleLineChange);
    document.getElementById('job_number').addEventListener('change', handleJobSelection);
    document.getElementById('main_category').addEventListener('change', handleMainCategoryChange);
    document.getElementById('start_time').addEventListener('change', updateDuration);
    document.getElementById('end_time').addEventListener('change', updateDuration);
    document.getElementById('crew-decrease').addEventListener('click', () => adjustCrewSize(-1));
    document.getElementById('crew-increase').addEventListener('click', () => adjustCrewSize(1));
    
    document.querySelectorAll('.btn-quick-note').forEach(btn => {
        btn.addEventListener('click', function() {
            const note = this.dataset.note;
            const commentsField = document.getElementById('comments');
            commentsField.value = (commentsField.value ? commentsField.value + '\n' : '') + note;
        });
    });
    
    document.getElementById('btn-clear').addEventListener('click', clearForm);
    document.getElementById('downtime-form').addEventListener('submit', handleFormSubmit);
}

// ============================================
// FACILITY & LINE HANDLING
// ============================================

function handleFacilityChange() {
    const facilityId = this.value;
    const lineSelect = document.getElementById('line_id');
    const jobSelect = document.getElementById('job_number');
    const jobCard = document.getElementById('job-details-card');
    
    lineSelect.innerHTML = `<option value="">${translations.SelectProductionLine}</option>`;
    lineSelect.disabled = true;
    
    jobSelect.innerHTML = `<option value="">${translations.SelectJob}</option>`;
    jobSelect.disabled = true;
    if (jobCard) jobCard.style.display = 'none';
    clearJobFields();
    
    if (!facilityId) return;
    
    fetch(`/downtime/api/lines/${facilityId}`)
        .then(response => response.json())
        .then(data => {
            if (data.success && data.lines) {
                data.lines.forEach(line => {
                    const option = document.createElement('option');
                    option.value = line.id;
                    option.textContent = line.name;
                    lineSelect.appendChild(option);
                });
                lineSelect.disabled = false;
            }
        })
        .catch(error => console.error('Error loading lines:', error));
}

function handleLineChange() {
    const lineId = this.value;
    currentLineId = lineId;
    
    const jobSelect = document.getElementById('job_number');
    const jobCard = document.getElementById('job-details-card');
    
    jobSelect.innerHTML = `<option value="">${translations.SelectJob}</option>`;
    jobSelect.disabled = true;
    if (jobCard) jobCard.style.display = 'none';
    clearJobFields();
    
    if (!lineId) return;
    
    loadTodaysEntries(lineId);
    loadJobsForLine();
}

// ============================================
// ERP JOB LOADING
// ============================================

function loadJobsForLine() {
    const facilitySelect = document.getElementById('facility_id');
    const lineSelect = document.getElementById('line_id');
    const jobSelect = document.getElementById('job_number');
    const jobLoading = document.getElementById('job-loading');
    
    const facilityName = facilitySelect.options[facilitySelect.selectedIndex].text;
    const lineName = lineSelect.options[lineSelect.selectedIndex].text;
    
    console.log(`📡 Loading ERP jobs for ${facilityName}/${lineName}`);
    
    if (jobLoading) jobLoading.style.display = 'block';
    
    fetch(`/api/erp/open-jobs/${encodeURIComponent(facilityName)}/${encodeURIComponent(lineName)}`)
        .then(response => response.json())
        .then(data => {
            if (jobLoading) jobLoading.style.display = 'none';
            
            if (data.success && data.jobs && data.jobs.length > 0) {
                availableJobs = data.jobs;
                console.log(`✅ Loaded ${data.jobs.length} jobs`);
                
                data.jobs.forEach(job => {
                    const option = document.createElement('option');
                    option.value = job.JobNumber;
                    option.textContent = `${job.JobNumber} - ${job.PartNumber}`;
                    option.dataset.jobData = JSON.stringify(job);
                    jobSelect.appendChild(option);
                });
                
                jobSelect.disabled = false;
            } else {
                console.log('ℹ️ No jobs found');
                const option = document.createElement('option');
                option.value = '';
                option.textContent = translations.NoOpenJobs;
                option.disabled = true;
                jobSelect.appendChild(option);
            }
        })
        .catch(error => {
            console.error('❌ Error loading jobs:', error);
            if (jobLoading) jobLoading.style.display = 'none';
            
            const option = document.createElement('option');
            option.value = '';
            option.textContent = translations.ErrorLoadingJobs;
            option.disabled = true;
            jobSelect.appendChild(option);
            
            jobSelect.disabled = false;
        });
}

function handleJobSelection() {
    const jobSelect = this;
    const selectedOption = jobSelect.options[jobSelect.selectedIndex];
    const jobCard = document.getElementById('job-details-card');
    
    if (jobSelect.value && selectedOption.dataset.jobData) {
        const jobData = JSON.parse(selectedOption.dataset.jobData);
        console.log('✅ Job selected:', jobData);
        
        document.getElementById('display-job-number').textContent = jobData.JobNumber || '-';
        document.getElementById('display-part-number').textContent = jobData.PartNumber || 'N/A';
        document.getElementById('display-part-description').textContent = jobData.PartDescription || 'N/A';
        document.getElementById('display-customer').textContent = jobData.Customer || 'N/A';
        document.getElementById('display-business-unit').textContent = jobData.s_BU || 'N/A';
        
        const salesOrderSection = document.getElementById('sales-order-section');
        if (jobData.SalesOrder && jobData.SalesOrder !== '') {
            document.getElementById('display-sales-order').textContent = jobData.SalesOrder;
            salesOrderSection.style.display = 'block';
        } else {
            salesOrderSection.style.display = 'none';
        }
        
        document.getElementById('erp_job_number').value = jobData.JobNumber || '';
        document.getElementById('erp_part_number').value = jobData.PartNumber || '';
        document.getElementById('erp_part_description').value = jobData.PartDescription || '';
        
        jobCard.style.display = 'block';
    } else {
        jobCard.style.display = 'none';
        clearJobFields();
    }
}

function clearJobFields() {
    document.getElementById('erp_job_number').value = '';
    document.getElementById('erp_part_number').value = '';
    document.getElementById('erp_part_description').value = '';
}

// ============================================
// CATEGORY HANDLING
// ============================================

function handleMainCategoryChange() {
    const mainCategoryId = parseInt(this.value);
    const subCategorySelect = document.getElementById('category_id');
    
    subCategorySelect.innerHTML = `<option value="">${translations.SelectSubCategory}</option>`;
    subCategorySelect.disabled = true;
    
    if (!mainCategoryId) return;
    
    const mainCategory = allCategories.find(cat => cat.category_id === mainCategoryId);
    if (!mainCategory || !mainCategory.subcategories) return;
    
    mainCategory.subcategories.forEach(sub => {
        const option = document.createElement('option');
        option.value = sub.category_id;
        option.textContent = `${sub.category_code} - ${sub.category_name}`;
        subCategorySelect.appendChild(option);
    });
    
    subCategorySelect.disabled = false;
}

// ============================================
// CREW SIZE ADJUSTMENT
// ============================================

function adjustCrewSize(delta) {
    const crewInput = document.getElementById('crew_size');
    let currentValue = parseInt(crewInput.value) || 2;
    let newValue = currentValue + delta;
    
    if (newValue >= 1 && newValue <= 10) {
        crewInput.value = newValue;
    }
}

// ============================================
// FORM SUBMISSION
// ============================================

function handleFormSubmit(e) {
    e.preventDefault();
    
    const formData = new FormData(this);
    const submitBtn = document.getElementById('btn-submit');
    
    submitBtn.disabled = true;
    submitBtn.innerHTML = `<svg class="icon-svg spinner-icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M304 48a48 48 0 1 0 -96 0 48 48 0 1 0 96 0zm0 416a48 48 0 1 0 -96 0 48 48 0 1 0 96 0zM48 304a48 48 0 1 0 0-96 48 48 0 1 0 0 96zm416 0a48 48 0 1 0 0-96 48 48 0 1 0 0 96zM142.9 437A48 48 0 1 0 75 369.1 48 48 0 1 0 142.9 437zm0-294.2A48 48 0 1 0 75 75a48 48 0 1 0 67.9 67.9zM369.1 437A48 48 0 1 0 437 369.1 48 48 0 1 0 369.1 437z"/></svg> ${translations.Submitting}`;
    
    fetch('/downtime/submit', {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showAlert(data.message, 'success');
            clearForm();
            if (currentLineId) {
                loadTodaysEntries(currentLineId);
            }
        } else {
            showAlert(data.message, 'error');
        }
    })
    .catch(error => {
        console.error('Error:', error);
        showAlert(translations.ErrorSubmitting, 'error');
    })
    .finally(() => {
        submitBtn.disabled = false;
        submitBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512"><path d="M48 96V416c0 8.8 7.2 16 16 16H384c8.8 0 16-7.2 16-16V170.5c0-4.2-1.7-8.3-4.7-11.3l-33.9-33.9c-3-3-7.1-4.7-11.3-4.7H64c-8.8 0-16 7.2-16 16zM128 32c0-17.7 14.3-32 32-32H288c17.7 0 32 14.3 32 32v64H128V32zM224 400a48 48 0 1 0 0-96 48 48 0 1 0 0 96z"/></svg> ${translations.SubmitDowntime}`;
    });
}

// ============================================
// ALERT SYSTEM
// ============================================

function showAlert(message, type) {
    const alertContainer = document.getElementById('alert-container');
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.textContent = message;
    alertContainer.appendChild(alertDiv);
    
    setTimeout(() => {
        alertDiv.style.opacity = '0';
        alertDiv.style.transition = 'opacity 0.3s';
        setTimeout(() => alertDiv.remove(), 300);
    }, 5000);
}

// ============================================
// FORM CLEARING
// ============================================

function clearForm() {
    document.getElementById('downtime-form').reset();
    document.getElementById('downtime_id').value = '';
    
    document.getElementById('line_id').innerHTML = `<option value="">${translations.SelectProductionLine}</option>`;
    document.getElementById('line_id').disabled = true;
    
    document.getElementById('job_number').innerHTML = `<option value="">${translations.SelectJob}</option>`;
    document.getElementById('job_number').disabled = true;
    
    const jobCard = document.getElementById('job-details-card');
    if (jobCard) jobCard.style.display = 'none';
    
    clearJobFields();
    
    document.getElementById('category_id').innerHTML = `<option value="">${translations.SelectMainFirst}</option>`;
    document.getElementById('category_id').disabled = true;
    
    initializeDateTimeFields();
    document.getElementById('crew_size').value = 2;
    
    console.log('✅ Form cleared');
}

// ============================================
// TODAY'S ENTRIES
// ============================================

function loadTodaysEntries(lineId) {
    fetch(`/downtime/api/today-entries/${lineId}`)
        .then(response => response.json())
        .then(data => {
            if (data.success && data.entries) {
                displayTodaysEntries(data.entries);
            }
        })
        .catch(error => console.error('Error loading entries:', error));
}

function displayTodaysEntries(entries) {
    const entriesSection = document.getElementById('todays-entries');
    const entriesList = document.getElementById('entries-list');
    
    if (!entries || entries.length === 0) {
        entriesSection.style.display = 'none';
        return;
    }
    
    entriesList.innerHTML = '';
    
    entries.forEach(entry => {
        const card = createEntryCard(entry);
        entriesList.appendChild(card);
    });
    
    entriesSection.style.display = 'block';
}

function createEntryCard(entry) {
    const div = document.createElement('div');
    div.className = `entry-card ${entry.is_own_entry ? 'own-entry' : 'other-entry'}`;
    
    const duration = entry.duration_minutes || 0;
    const enteredBy = entry.entered_by || 'Unknown';
    
    div.innerHTML = `
        <div class="entry-header">
            <strong>${entry.category_name}</strong>
            <span>${duration} ${translations.min}</span>
        </div>
        <div class="entry-details">
            ${entry.start_time_str} - ${entry.end_time_str} | 
            ${entry.shift_name || 'N/A'} | 
            ${translations.Crew}: ${entry.crew_size || 1}
            ${!entry.is_own_entry ? ` | 👤 ${enteredBy}` : ''}
        </div>
        ${entry.reason_notes ? `<div class="entry-notes">${entry.reason_notes}</div>` : ''}
        ${entry.is_own_entry ? `
            <div class="entry-actions">
                <button onclick="editEntry(${entry.downtime_id})" class="btn btn-secondary btn-sm">
                    <svg class="icon-svg" style="width: 14px; height: 14px;" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><path d="M471.6 21.7c-21.9-21.9-57.3-21.9-79.2 0L362.3 51.7l97.9 97.9 30.1-30.1c21.9-21.9 21.9-57.3 0-79.2L471.6 21.7zm-299.2 220c-6.1 6.1-10.8 13.6-13.5 21.9l-29.6 88.8c-2.9 8.6-.6 18.1 5.8 24.6s15.9 8.7 24.6 5.8l88.8-29.6c8.2-2.7 15.7-7.4 21.9-13.5L437.7 172.3 339.7 74.3 172.4 241.7zM96 64C43 64 0 107 0 160V416c0 53 43 96 96 96H352c53 0 96-43 96-96V320c0-17.7-14.3-32-32-32s-32 14.3-32 32v96c0 17.7-14.3 32-32 32H96c-17.7 0-32-14.3-32-32V160c0-17.7 14.3-32 32-32h96c17.7 0 32-14.3 32-32s-14.3-32-32-32H96z"/></svg>
                    ${translations.Edit}
                </button>
                <button onclick="deleteEntry(${entry.downtime_id})" class="btn btn-secondary btn-sm" style="border-color: var(--accent-red); color: var(--accent-red);">
                     <svg class="icon-svg" style="width: 14px; height: 14px;" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512"><path d="M135.2 17.7L128 32H32C14.3 32 0 46.3 0 64S14.3 96 32 96H416c17.7 0 32-14.3 32-32s-14.3-32-32-32H320l-7.2-14.3C307.4 6.8 296.3 0 284.2 0H163.8c-12.1 0-23.2 6.8-28.6 17.7zM416 128H32L53.2 467c1.6 25.3 22.6 45 47.9 45H346.9c25.3 0 46.3-19.7 47.9-45L416 128z"/></svg>
                    ${translations.Delete}
                </button>
            </div>
        ` : ''}
    `;
    
    return div;
}

function editEntry(downtimeId) {
    console.log('Edit entry:', downtimeId);
    showAlert(translations.EditNotImplemented, 'info');
}

function deleteEntry(downtimeId) {
    if (!confirm(translations.DeleteConfirm)) return;
    
    fetch(`/downtime/delete/${downtimeId}`, {
        method: 'POST'
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showAlert(translations.EntryDeleted, 'success');
            if (currentLineId) {
                loadTodaysEntries(currentLineId);
            }
        } else {
            showAlert(data.message, 'error');
        }
    })
    .catch(error => {
        console.error('Error:', error);
        showAlert(translations.ErrorDeleting, 'error');
    });
}

console.log('✅ Downtime entry form script loaded');