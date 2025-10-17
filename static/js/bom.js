// dangquyenbui-dotcom/downtime_tracker/downtime_tracker-953d9e6915ad7fa465db9a8f87b8a56d713b0537/static/js/bom.js
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('bomSearch');
    const exportButton = document.getElementById('exportBtn');

    if (searchInput) {
        const debouncedSearch = dtUtils.debounce(filterBomTable, 250);
        searchInput.addEventListener('keyup', debouncedSearch);

        // Run initial search and row count update
        filterBomTable();
    }
    
    if (exportButton) {
        exportButton.addEventListener('click', exportVisibleDataToXlsx);
    }
});

function updateRowCount() {
    const totalRows = document.querySelectorAll('#bomTableBody tr').length;
    const visibleRows = document.querySelectorAll('#bomTableBody tr:not([style*="display: none"])').length;
    const rowCountEl = document.getElementById('rowCount');
    if (rowCountEl) {
        rowCountEl.textContent = `Showing ${visibleRows} of ${totalRows} rows`;
    }
}

function filterBomTable() {
    const searchTerm = document.getElementById('bomSearch').value.toLowerCase();
    const rows = document.querySelectorAll('#bomTableBody tr');
    
    rows.forEach(row => {
        const parentPart = row.cells[0].textContent.toLowerCase();
        const componentPart = row.cells[2].textContent.toLowerCase();
        const description = row.cells[3].textContent.toLowerCase();
        
        const rowText = `${parentPart} ${componentPart} ${description}`;
        
        if (rowText.includes(searchTerm)) {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });

    // Update the count after filtering
    updateRowCount();
}

function exportVisibleDataToXlsx() {
    const exportBtn = document.getElementById('exportBtn');
    exportBtn.disabled = true;
    exportBtn.textContent = '📥 Generating...';

    const headers = Array.from(document.querySelectorAll('.table thead th'))
        .map(th => th.textContent.trim());

    const rows = [];
    document.querySelectorAll('#bomTableBody tr:not([style*="display: none"])').forEach(row => {
        const rowData = Array.from(row.querySelectorAll('td')).map(td => td.textContent.trim());
        rows.push(rowData);
    });

    if (rows.length === 0) {
        dtUtils.showAlert('No data to export.', 'info');
        exportBtn.disabled = false;
        exportBtn.textContent = '📥 Download XLSX';
        return;
    }
    
    fetch('/bom/api/export-xlsx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ headers, rows })
    })
    .then(response => {
        if (!response.ok) { throw new Error('Network response was not ok.'); }
        const disposition = response.headers.get('Content-Disposition');
        const matches = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/.exec(disposition);
        const filename = (matches != null && matches[1]) ? matches[1].replace(/['"]/g, '') : 'bom_export.xlsx';
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