"""
Database operations for generating analytical reports.
"""

from .connection import get_db
from datetime import datetime, timedelta
from .mrp_service import mrp_service

class ReportsDB:
    """Reporting database operations"""

    def __init__(self):
        self.db = get_db()

    def get_downtime_summary(self, start_date, end_date, facility_id=None, line_id=None):
        """
        Generates aggregated data for the downtime summary report.
        """
        with self.db.get_connection() as conn:
            
            # Base query and params
            base_sql = """
                FROM Downtimes d
                JOIN ProductionLines pl ON d.line_id = pl.line_id
                JOIN Facilities f ON pl.facility_id = f.facility_id
                JOIN DowntimeCategories dc ON d.category_id = dc.category_id
                WHERE d.is_deleted = 0
                AND d.start_time BETWEEN ? AND ?
            """
            params = [start_date, end_date]

            # Append filters
            if facility_id:
                base_sql += " AND f.facility_id = ? "
                params.append(facility_id)
            if line_id:
                base_sql += " AND pl.line_id = ? "
                params.append(line_id)

            # 1. Overall Stats
            stats_query = f"SELECT COUNT(*) as total_events, SUM(d.duration_minutes) as total_minutes {base_sql}"
            overall_stats = conn.execute_query(stats_query, params)

            # 2. Downtime by Category
            category_query = f"""
                SELECT 
                    dc.category_name, 
                    dc.color_code,
                    SUM(d.duration_minutes) as total_minutes
                {base_sql}
                GROUP BY dc.category_name, dc.color_code
                ORDER BY total_minutes DESC
            """
            by_category = conn.execute_query(category_query, params)

            # 3. Downtime by Production Line
            line_query = f"""
                SELECT 
                    pl.line_name,
                    SUM(d.duration_minutes) as total_minutes
                {base_sql}
                GROUP BY pl.line_name
                ORDER BY total_minutes DESC
            """
            by_line = conn.execute_query(line_query, params)
            
            # 4. Raw data for table view
            raw_data_query = f"""
                SELECT TOP 250
                    d.start_time,
                    d.duration_minutes,
                    f.facility_name,
                    pl.line_name,
                    dc.category_name,
                    d.entered_by,
                    d.reason_notes
                {base_sql}
                ORDER BY d.start_time DESC
            """
            raw_data = conn.execute_query(raw_data_query, params)

            # Calculate average
            total_events = overall_stats[0]['total_events'] if overall_stats else 0
            total_minutes = overall_stats[0]['total_minutes'] if overall_stats else 0
            avg_duration = (total_minutes / total_events) if total_events > 0 else 0

            return {
                'overall_stats': {
                    'total_events': total_events,
                    'total_minutes': total_minutes or 0,
                    'avg_duration': round(avg_duration, 1)
                },
                'by_category': by_category,
                'by_line': by_line,
                'raw_data': raw_data
            }

    def get_shipment_forecast(self):
        """
        Generates an automated shipment forecast for the current month based on MRP results.
        """
        mrp_results = mrp_service.calculate_mrp_suggestions()
        
        today = datetime.now()
        # Find the last day of the current month
        next_month = today.replace(day=28) + timedelta(days=4)
        last_day_of_month = next_month - timedelta(days=next_month.day)

        forecast = {
            'month_name': today.strftime('%B %Y'),
            'likely_total_value': 0,
            'at_risk_total_value': 0,
            'likely_orders': [],
            'at_risk_orders': []
        }

        for result in mrp_results:
            so = result['sales_order']
            due_date_str = so.get('Due to Ship')
            
            if not due_date_str:
                continue

            try:
                due_date = datetime.strptime(due_date_str, '%m/%d/%Y')
                # Production must finish 2 days before due date to allow for QC and shipping
                required_completion_date = due_date - timedelta(days=2)

                # Only consider orders that can be completed this month
                if not (today.month == required_completion_date.month and today.year == required_completion_date.year):
                    continue

                unit_price = so.get('Unit Price', 0) or 0
                status = result['status']
                
                order_details_base = {
                    'so_number': so['SO'],
                    'customer_name': so['Customer Name'],
                    'part_number': so['Part'],
                    'due_date': due_date.strftime('%Y-%m-%d'),
                    'status': result.get('bottleneck', status),
                    'calculation_breakdown': []
                }

                # --- REVISED LOGIC ---

                # High-confidence shipments (Ready, Producible in Full, or Pending QC)
                if status in ['ready-to-ship', 'ok', 'pending-qc']:
                    ord_qty = (so.get('Ord Qty - Cur. Level', 0) or 0)
                    shippable_value = ord_qty * unit_price
                    forecast['likely_total_value'] += shippable_value
                    details = order_details_base.copy()
                    details['shippable_value'] = shippable_value
                    details['calculation_breakdown'].append(f"Full Order: {ord_qty:,.0f} units x ${unit_price:,.2f}/unit")
                    forecast['likely_orders'].append(details)

                # Partial shipments (split between likely and at-risk)
                elif status == 'partial-ship':
                    # The portion already in stock is "Likely"
                    if result['shippable_qty'] > 0:
                        shippable_value = result['shippable_qty'] * unit_price
                        forecast['likely_total_value'] += shippable_value
                        details = order_details_base.copy()
                        details['shippable_value'] = shippable_value
                        details['status'] = 'Partial Ship (from Stock)'
                        details['calculation_breakdown'].append(f"From Stock: {result['shippable_qty']:,.0f} units x ${unit_price:,.2f}/unit")
                        forecast['likely_orders'].append(details)
                        
                    # The producible portion is now also "Likely"
                    if result['producible_qty'] > 0:
                        producible_value = result['producible_qty'] * unit_price
                        forecast['likely_total_value'] += producible_value
                        details_prod = order_details_base.copy()
                        details_prod['shippable_value'] = producible_value
                        details_prod['status'] = 'Partial Ship (Producible)'
                        details_prod['calculation_breakdown'].append(f"Producible: {result['producible_qty']:,.0f} units x ${unit_price:,.2f}/unit")
                        forecast['likely_orders'].append(details_prod)
                
                # At-risk shipments (Producible in part)
                elif status == 'partial':
                    if result['producible_qty'] > 0:
                        producible_value = result['producible_qty'] * unit_price
                        forecast['at_risk_total_value'] += producible_value
                        details = order_details_base.copy()
                        details['shippable_value'] = producible_value
                        details['calculation_breakdown'].append(f"Producible: {result['producible_qty']:,.0f} units x ${unit_price:,.2f}/unit")
                        forecast['at_risk_orders'].append(details)

            except (ValueError, TypeError):
                continue
        
        return forecast


# Singleton instance
reports_db = ReportsDB()