// dangquyenbui-dotcom/downtime_tracker/downtime_tracker-953d9e6915ad7fa465db9a8f87b8a56d713b0537/static/js/po.js
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('poSearch');
    const exportButton = document.getElementById('exportBtn');

    if (searchInput) {
        const debouncedSearch = dtUtils.debounce(filterPoTable, 250);
        searchInput.addEventListener('keyup', debouncedSearch);
        filterPoTable(); // Initial count
    }
    
    if (exportButton) {
        exportButton.addEventListener('click', exportVisibleDataToXlsx);
    }
});

function updateRowCount() {
    const totalRows = document.querySelectorAll('#poTableBody tr').length;
    const visibleRows = document.querySelectorAll('#poTableBody tr:not([style*="display: none"])').length;
    const rowCountEl = document.getElementById('rowCount');
    if (rowCountEl) {
        rowCountEl.textContent = `Showing ${visibleRows} of ${totalRows} rows`;
    }
}

function filterPoTable() {
    const searchTerm = document.getElementById('poSearch').value.toLowerCase();
    const rows = document.querySelectorAll('#poTableBody tr');
    
    rows.forEach(row => {
        // Search across PO #, Part #, Description, and Vendor
        const poNumber = row.cells[0].textContent.toLowerCase();
        const partNumber = row.cells[1].textContent.toLowerCase();
        const description = row.cells[2].textContent.toLowerCase();
        const vendor = row.cells[3].textContent.toLowerCase();
        
        const rowText = `${poNumber} ${partNumber} ${description} ${vendor}`;
        
        if (rowText.includes(searchTerm)) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });

    updateRowCount();
}

function exportVisibleDataToXlsx() {
    const exportBtn = document.getElementById('exportBtn');
    exportBtn.disabled = true;
    exportBtn.textContent = '📥 Generating...';

    const headers = Array.from(document.querySelectorAll('.table thead th'))
        .map(th => th.textContent.trim());

    const rows = [];
    document.querySelectorAll('#poTableBody tr:not([style*="display: none"])').forEach(row => {
        const rowData = Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim());
        rows.push(rowData);
    });

    if (rows.length === 0) {
        dtUtils.showAlert('No data to export.', 'info');
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
        return;
    }
    
    fetch('/po/api/export-xlsx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ headers, rows })
    })
    .then(response => {
        if (!response.ok) { throw new Error('Network response was not ok.'); }
        const disposition = response.headers.get('Content-Disposition');
        const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(disposition);
        const filename = (matches != null && matches[1]) ? matches[1].replace(/['"]/g, '') : 'po_export.xlsx';
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