/**
 * Common JavaScript Functions
 * Shared utilities across all pages
 */

// Alert System
function showAlert(message, type = 'info') {
    const alertsDiv = document.getElementById('alerts') || createAlertsContainer();
    const alertClass = type === 'success' ? 'alert-success' : 
                      type === 'error' ? 'alert-error' : 
                      'alert-info';
    
    const alertElement = document.createElement('div');
    alertElement.className = `alert ${alertClass}`;
    alertElement.textContent = message;
    
    alertsDiv.appendChild(alertElement);
    
    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        alertElement.style.opacity = '0';
        setTimeout(() => alertElement.remove(), 500); // Match CSS transition
    }, 5000);
}

function createAlertsContainer() {
    const container = document.querySelector('.container');
    const alertsDiv = document.createElement('div');
    alertsDiv.id = 'alerts';
    container.insertBefore(alertsDiv, container.firstChild);
    return alertsDiv;
}

// Modal Management
function openModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.style.display = 'block';
        // Focus first input if exists
        const firstInput = modal.querySelector('input:not([type="hidden"]), select, textarea');
        if (firstInput) {
            setTimeout(() => firstInput.focus(), 100);
        }
    }
}

function closeModal(modalId) {
    const modal = document.getElementById(modalId);
    if (modal) {
        modal.style.display = 'none';
    }
}

// Close modal when clicking outside
window.addEventListener('click', function(event) {
    if (event.target.classList.contains('modal')) {
        event.target.style.display = 'none';
    }
});

// Close modal with Escape key
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        const openModals = document.querySelectorAll('.modal[style*="block"]');
        openModals.forEach(modal => modal.style.display = 'none');
    }
});

// Form Utilities
function resetForm(formId) {
    const form = document.getElementById(formId);
    if (form) {
        form.reset();
        // Clear any validation messages
        form.querySelectorAll('.error-message').forEach(el => el.remove());
    }
}

function submitForm(formId, url, onSuccess, onError) {
    const form = document.getElementById(formId);
    if (!form) return;
    
    const formData = new FormData(form);
    
    fetch(url, {
        method: 'POST',
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showAlert(data.message, 'success');
            if (onSuccess) onSuccess(data);
        } else {
            showAlert(data.message, 'error');
            if (onError) onError(data);
        }
    })
    .catch(error => {
        showAlert('An error occurred. Please try again.', 'error');
        console.error('Form submission error:', error);
        if (onError) onError({ error: error.toString() });
    });
}

// Table Filtering
function filterTable(tableId, filters) {
    const table = document.getElementById(tableId);
    if (!table) return;
    
    const rows = table.querySelectorAll('tbody tr');
    let visibleCount = 0;
    
    rows.forEach(row => {
        let show = true;
        
        for (const [attribute, value] of Object.entries(filters)) {
            if (value && row.getAttribute(`data-${attribute}`) !== value) {
                show = false;
                break;
            }
        }
        
        row.style.display = show ? '' : 'none';
        if (show) visibleCount++;
    });
    
    return visibleCount;
}

// Confirmation Dialog
function confirmAction(message, onConfirm, onCancel) {
    if (confirm(message)) {
        if (onConfirm) onConfirm();
    } else {
        if (onCancel) onCancel();
    }
}

// Delete with Confirmation
function deleteItem(url, itemName, onSuccess) {
    confirmAction(
        `Are you sure you want to delete "${itemName}"?`,
        () => {
            fetch(url, { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        showAlert(data.message, 'success');
                        if (onSuccess) onSuccess(data);
                        // Reload after 1 second
                        setTimeout(() => location.reload(), 1000);
                    } else {
                        showAlert(data.message, 'error');
                    }
                })
                .catch(error => {
                    showAlert('Failed to delete item', 'error');
                    console.error('Delete error:', error);
                });
        }
    );
}

// Format Date/Time
function formatDateTime(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
}

function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleDateString();
}

// Debounce Function
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Loading Indicator
function showLoading(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = '<div class="loading">Loading...</div>';
    }
}

function hideLoading(elementId, content = '') {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = content;
    }
}

// Export functions for use
window.dtUtils = {
    showAlert,
    openModal,
    closeModal,
    resetForm,
    submitForm,
    filterTable,
    confirmAction,
    deleteItem,
    formatDateTime,
    formatDate,
    debounce,
    showLoading,
    hideLoading
};

// Auto-dismiss server-rendered flash messages on page load
document.addEventListener('DOMContentLoaded', function() {
    const alerts = document.querySelectorAll('#alerts .alert');
    alerts.forEach(function(alert) {
        setTimeout(function() {
            alert.style.opacity = '0';
            setTimeout(function() {
                // Check if the parent #alerts container is empty and remove it
                alert.remove();
                const alertsContainer = document.getElementById('alerts');
                if (alertsContainer && !alertsContainer.hasChildNodes()) {
                    alertsContainer.remove();
                }
            }, 500); // Should match CSS transition duration
        }, 5000); // 5 seconds
    });
});