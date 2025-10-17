/**
 * Theme Management System
 * Handles dark/light theme switching with persistence
 */

(function() {
    'use strict';
    
    /**
     * Updates the page's favicon based on the current theme.
     * @param {string} theme - The current theme ('dark' or 'light').
     */
    function updateFavicon(theme) {
        const favicon = document.getElementById('favicon');
        if (favicon && window.FAVICON_PATHS) {
            favicon.setAttribute('href', theme === 'dark' ? window.FAVICON_PATHS.dark : window.FAVICON_PATHS.light);
        }
    }
    
    // Show body after everything is loaded
    document.addEventListener('DOMContentLoaded', function() {
        document.body.classList.add('loaded');
        
        // Initialize theme toggle if it exists
        initializeTheme();
    });
    
    function initializeTheme() {
        const themeToggle = document.getElementById('themeToggle');
        if (!themeToggle) return;
        
        const htmlElement = document.documentElement;
        
        // Theme is already set in the head, just sync the toggle
        const currentTheme = htmlElement.getAttribute('data-theme') || 'light';
        themeToggle.checked = currentTheme === 'dark';
        
        // The initial favicon is set by an inline script in the <head> to prevent flashing.
        // This script handles changes *after* the page loads.
        
        // Handle theme toggle
        themeToggle.addEventListener('change', function() {
            const theme = this.checked ? 'dark' : 'light';
            htmlElement.setAttribute('data-theme', theme);
            localStorage.setItem('theme', theme);
            updateFavicon(theme); // Update favicon on toggle
            
            // Notify other parts of the app (like charts) about the theme change
            window.dispatchEvent(new CustomEvent('themeChange', { detail: theme }));
        });
        
        // Listen for theme changes from other tabs
        window.addEventListener('storage', function(e) {
            if (e.key === 'theme') {
                const theme = e.newValue || 'light';
                htmlElement.setAttribute('data-theme', theme);
                themeToggle.checked = theme === 'dark';
                updateFavicon(theme); // Update favicon from storage event
            }
        });
        
        // Listen for custom theme change events within the same tab/window
        window.addEventListener('themeChange', function(e) {
            const theme = e.detail;
            htmlElement.setAttribute('data-theme', theme);
            themeToggle.checked = theme === 'dark';
            updateFavicon(theme); // Update favicon from custom event
        });
    }
})();