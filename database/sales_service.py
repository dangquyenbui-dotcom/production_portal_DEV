# database/sales_service.py
"""
Service for sales-related data analysis and reporting.
"""
# UPDATED IMPORT:
from database import get_erp_service
from datetime import datetime, timedelta

class SalesService:
    def __init__(self):
        # This call remains the same
        self.erp = get_erp_service()

    def get_all_customers(self):
        """Gets a list of all unique customer names."""
        open_orders = self.erp.get_open_order_schedule() # Call remains the same
        customers = sorted(list(set(order['Customer Name'] for order in open_orders if order.get('Customer Name'))))
        return customers

    def get_customer_analysis(self, customer_name):
        """
        Generates a full analysis for a given customer.
        """
        all_orders = self.erp.get_open_order_schedule() # Call remains the same
        customer_orders = [order for order in all_orders if order.get('Customer Name') == customer_name]

        # --- KPIs ---
        ytd_sales = sum(order.get('Ext $ (Current x Price)', 0) for order in customer_orders if self._is_ytd(order.get('Ordered Date')))
        open_order_value = sum(order.get('Ext $ (Net Qty x Price)', 0) for order in customer_orders)
        total_open_orders = len(customer_orders)
        avg_order_value = open_order_value / total_open_orders if total_open_orders > 0 else 0

        # --- Sales Trends (dummy data for example) ---
        sales_trend = self._get_dummy_sales_trend()

        # --- Top Products ---
        top_products = self._calculate_top_products(customer_orders)

        # --- Recent Shipments (dummy data for example) ---
        recent_shipments = self._get_dummy_recent_shipments()

        return {
            'kpis': {
                'ytd_sales': ytd_sales,
                'open_order_value': open_order_value,
                'total_open_orders': total_open_orders,
                'avg_order_value': avg_order_value,
            },
            'sales_trend': sales_trend,
            'top_products': top_products,
            'recent_shipments': recent_shipments,
            'open_orders': customer_orders,
        }

    def _is_ytd(self, date_str):
        if not date_str: return False
        try:
            order_date = datetime.strptime(date_str, '%m/%d/%Y')
            return order_date.year == datetime.now().year
        except (ValueError, TypeError): return False

    def _calculate_top_products(self, orders):
        product_sales = {}
        for order in orders:
            part = order.get('Part')
            value = order.get('Ext $ (Net Qty x Price)', 0)
            if part: product_sales[part] = product_sales.get(part, 0) + value
        sorted_products = sorted(product_sales.items(), key=lambda item: item[1], reverse=True)
        return sorted_products[:5]

    def _get_dummy_sales_trend(self):
        return {
            'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'],
            'data': [120000, 150000, 180000, 160000, 210000, 250000, 230000, 280000, 310000]
        }

    def _get_dummy_recent_shipments(self):
        return [
            {'so': 'S12345', 'ship_date': '2025-09-28', 'value': 75000},
            {'so': 'S12300', 'ship_date': '2025-09-15', 'value': 120000},
            {'so': 'S12250', 'ship_date': '2025-08-30', 'value': 95000},
        ]

# Singleton instance
sales_service = SalesService()