"""
Internationalization Configuration for Production Portal
Supports US English and US Spanish
Compatible with Flask-Babel 3.0+
"""

import os
from flask import request, session
from flask_babel import Babel, lazy_gettext, gettext, ngettext

# Initialize Babel instance
babel = Babel()

class I18nConfig:
    """Internationalization configuration and management"""
    
    # Supported languages
    LANGUAGES = {
        'en': {'name': 'English', 'flag': '🇺🇸', 'locale': 'en_US'},
        'es': {'name': 'Español', 'flag': '🇲🇽', 'locale': 'es_MX'}
    }
    
    # Default language
    DEFAULT_LANGUAGE = 'en'
    
    # Babel configuration
    BABEL_DEFAULT_LOCALE = 'en'
    BABEL_DEFAULT_TIMEZONE = 'America/Los_Angeles'
    BABEL_TRANSLATION_DIRECTORIES = 'translations'
    
    @staticmethod
    def init_app(app):
        """Initialize Flask-Babel with the application"""
        # Configure Babel settings
        app.config['BABEL_DEFAULT_LOCALE'] = I18nConfig.BABEL_DEFAULT_LOCALE
        app.config['BABEL_DEFAULT_TIMEZONE'] = I18nConfig.BABEL_DEFAULT_TIMEZONE
        app.config['BABEL_TRANSLATION_DIRECTORIES'] = I18nConfig.BABEL_TRANSLATION_DIRECTORIES
        app.config['LANGUAGES'] = I18nConfig.LANGUAGES
        
        # Initialize Babel with the app
        babel.init_app(app, locale_selector=I18nConfig.get_locale)
        
        return babel
    
    @staticmethod
    def get_locale():
        """Select the best language based on user preference"""
        # 1. Check if user forced a language change
        if 'language_override' in session:
            lang = session.pop('language_override')
            session['language'] = lang
            # Save to database if user is logged in
            if 'user' in session:
                I18nConfig.save_user_language(session['user']['username'], lang)
            return lang
        
        # 2. Check session for saved language preference
        if 'language' in session:
            return session['language']
        
        # 3. Check database for user's saved preference
        if 'user' in session:
            saved_lang = I18nConfig.get_user_language(session['user']['username'])
            if saved_lang:
                session['language'] = saved_lang
                return saved_lang
        
        # 4. Default to English
        return I18nConfig.DEFAULT_LANGUAGE
    
    @staticmethod
    def get_user_language(username):
        """Get user's saved language preference from database"""
        try:
            from database.users import UsersDB
            users_db = UsersDB()
            user_prefs = users_db.get_user_preference(username, 'language')
            return user_prefs if user_prefs in I18nConfig.LANGUAGES else None
        except:
            return None
    
    @staticmethod
    def save_user_language(username, language):
        """Save user's language preference to database"""
        try:
            from database.users import UsersDB
            users_db = UsersDB()
            users_db.set_user_preference(username, 'language', language)
            return True
        except:
            return False
    
    @staticmethod
    def get_available_languages():
        """Get list of available languages for display"""
        return [
            {
                'code': code,
                'name': info['name'],
                'flag': info['flag'],
                'locale': info['locale']
            }
            for code, info in I18nConfig.LANGUAGES.items()
        ]
    
    @staticmethod
    def switch_language(language_code):
        """Switch to a different language"""
        if language_code in I18nConfig.LANGUAGES:
            session['language_override'] = language_code
            return True
        return False

# Convenience imports for templates and routes
_ = gettext  # For simple translations
_l = lazy_gettext  # For lazy evaluation (forms, etc.)
_n = ngettext  # For pluralization

def format_datetime_i18n(dt, format='medium'):
    """
    Format datetime with localization
    
    Args:
        dt: datetime object to format
        format: 'short', 'medium', 'long', or 'full'
    
    Returns:
        str: Formatted datetime string
    """
    if dt is None:
        return ''
    try:
        from flask_babel import format_datetime
        return format_datetime(dt, format=format)
    except:
        # Fallback if format_datetime fails
        return str(dt)

def format_date_i18n(dt, format='medium'):
    """
    Format date with localization
    
    Args:
        dt: date object to format
        format: 'short', 'medium', 'long', or 'full'
    
    Returns:
        str: Formatted date string
    """
    if dt is None:
        return ''
    try:
        from flask_babel import format_date
        return format_date(dt, format=format)
    except:
        # Fallback if format_date fails
        return str(dt)