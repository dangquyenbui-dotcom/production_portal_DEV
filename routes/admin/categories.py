"""
Categories management routes with full database integration
UPDATED: Added reactivation functionality
"""

from flask import Blueprint, render_template, redirect, url_for, session, jsonify, request, flash
from auth import require_login, require_admin
from routes.main import validate_session
from database import categories_db, audit_db
from utils import get_client_info


admin_categories_bp = Blueprint('admin_categories', __name__)

@admin_categories_bp.route('/categories')
@validate_session
def categories():
    if not require_login(session):
        return redirect(url_for('main.login'))
    
    if not require_admin(session):
        flash('Admin privileges required', 'error')
        return redirect(url_for('main.dashboard'))
    
    # Get categories from database
    categories = categories_db.get_hierarchical(active_only=False)
    
    return render_template('admin/categories.html', 
                         categories=categories, 
                         user=session['user'])

@admin_categories_bp.route('/categories/add', methods=['POST'])
@validate_session
def add_category():
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        category_code = request.form.get('category_code', '').strip().upper()
        category_name = request.form.get('category_name', '').strip()
        parent_id = request.form.get('parent_id', '').strip() or None
        description = request.form.get('description', '').strip()
        color_code = request.form.get('color_code', '#667eea').strip()
        notification_required = request.form.get('notification_required') == 'true'
        
        if not category_code or not category_name:
            return jsonify({'success': False, 'message': 'Category code and name are required'})
        
        # Convert parent_id to int if provided
        if parent_id:
            try:
                parent_id = int(parent_id)
            except ValueError:
                parent_id = None
        
        # Create category
        success, message, category_id = categories_db.create(
            category_name, category_code, description, parent_id,
            color_code, notification_required, session['user']['username']
        )
        
        if success and category_id:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='DowntimeCategories',
                record_id=category_id,
                action_type='INSERT',
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes=f"Created category: {category_name} ({category_code})"
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error adding category: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_categories_bp.route('/categories/edit/<int:category_id>', methods=['POST'])
@validate_session
def edit_category(category_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        category_name = request.form.get('category_name', '').strip()
        description = request.form.get('description', '').strip()
        color_code = request.form.get('color_code', '#667eea').strip()
        notification_required = request.form.get('notification_required') == 'true'
        
        if not category_name:
            return jsonify({'success': False, 'message': 'Category name is required'})
        
        # Update category
        success, message, changes = categories_db.update(
            category_id, category_name, description, color_code,
            notification_required, session['user']['username']
        )
        
        if success and changes:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='DowntimeCategories',
                record_id=category_id,
                action_type='UPDATE',
                changes=changes,
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error editing category: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_categories_bp.route('/categories/delete/<int:category_id>', methods=['POST'])
@validate_session
def delete_category(category_id):
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Deactivate category
        success, message = categories_db.deactivate(
            category_id, session['user']['username']
        )
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='DowntimeCategories',
                record_id=category_id,
                action_type='DEACTIVATE',
                changes={'is_active': {'old': '1', 'new': '0'}},
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error deactivating category: {str(e)}")
        return jsonify({'success': False, 'message': 'An error occurred'})

@admin_categories_bp.route('/categories/reactivate/<int:category_id>', methods=['POST'])
@validate_session
def reactivate_category(category_id):
    """Reactivate an inactive category"""
    if not require_login(session) or not require_admin(session):
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    try:
        # Reactivate category
        success, message = categories_db.reactivate(
            category_id, session['user']['username']
        )
        
        if success:
            # Log in audit
            ip, user_agent = get_client_info()
            audit_db.log(
                table_name='DowntimeCategories',
                record_id=category_id,
                action_type='REACTIVATE',
                changes={'is_active': {'old': '0', 'new': '1'}},
                username=session['user']['username'],
                ip=ip,
                user_agent=user_agent,
                notes='Category reactivated'
            )
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        print(f"Error reactivating category: {str(e)}")
        return jsonify({'success': False, 'message': f'An error occurred: {str(e)}'})