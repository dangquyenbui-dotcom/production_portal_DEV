# dangquyenbui-dotcom/production_portal_dev/production_portal_DEV-1d426cd38b2291765ba776ab12103b173b271ab9/database/mrp_service.py
"""
MRP (Material Requirements Planning) Service
This service contains the core logic for calculating production suggestions.
"""

from .erp_connection import get_erp_service
from .capacity import ProductionCapacityDB
from datetime import datetime

# Create an instance of the capacity DB directly
capacity_db = ProductionCapacityDB()

class MRPService:
    def __init__(self):
        self.erp = get_erp_service()

    def get_component_inventory(self):
        """
        Fetches and processes raw material/component inventory from the ERP.
        Returns a dictionary mapping part numbers to their available quantities.
        """
        inventory_data = self.erp.get_raw_material_inventory()
        inventory = {}
        for row in inventory_data:
            part_number = row['PartNumber']
            inventory[part_number] = {
                'approved': row.get('on_hand_approved', 0),
                'pending_qc': row.get('on_hand_pending_qc', 0),
                'quarantine': row.get('on_hand_quarantine', 0),
                'issued_to_job': row.get('issued_to_job', 0),
                'staged': row.get('staged', 0)
            }
        return inventory

    def calculate_mrp_suggestions(self):
        """
        The main MRP engine. Calculates production suggestions for all open sales orders.
        """
        # 1. Fetch all necessary data in bulk
        print("MRP RUN: Fetching data...")
        sales_orders = self.erp.get_open_order_schedule()
        boms = self.erp.get_bom_data()
        purchase_orders = self.erp.get_purchase_order_data()
        component_inventory = self.get_component_inventory()
        finished_good_inventory_data = self.erp.get_on_hand_inventory()
        capacities = {c['line_id']: c['capacity_per_shift'] for c in capacity_db.get_all()}
        
        open_jobs = self.erp.get_open_production_jobs()
        jobs_by_so = {}
        for job in open_jobs:
            so_num = str(job.get('so_number'))
            if so_num:
                if so_num not in jobs_by_so:
                    jobs_by_so[so_num] = []
                jobs_by_so[so_num].append({
                    "jo_jobnum": job['jo_jobnum'],
                    "job_quantity": job.get('job_quantity', 0) or 0,
                    "completed_quantity": job.get('completed_quantity', 0) or 0
                })

        # 2. Pre-process and create lookups
        fg_inventory_map = {
            item['PartNumber'].strip(): {
                'approved': item.get('on_hand_approved', 0),
                'pending_qc': item.get('on_hand_pending_qc', 0),
                'total': item.get('TotalOnHand', 0)
            } for item in finished_good_inventory_data
        }
        
        boms_by_parent = {}
        for item in boms:
            parent = item['Parent Part Number'].strip()
            if parent not in boms_by_parent:
                boms_by_parent[parent] = []
            boms_by_parent[parent].append(item)

        pos_by_part = {}
        for po in purchase_orders:
            part = po['Part Number'].strip()
            open_qty = po.get('OpenPOQuantity', 0)
            if open_qty > 0:
                if part not in pos_by_part:
                    pos_by_part[part] = 0
                pos_by_part[part] += open_qty

        # 3. Initialize mutable "live" inventories for sequential allocation
        live_fg_approved = {part.strip(): data.get('approved', 0) for part, data in fg_inventory_map.items()}
        live_fg_qc = {part.strip(): data.get('pending_qc', 0) for part, data in fg_inventory_map.items()}

        # 4. Sort Sales Orders by "Due to Ship" date to process them in priority order
        max_date = datetime.max.date()
        def get_sort_date(so):
            due_date_str = so.get('Due to Ship')
            if due_date_str:
                try:
                    return datetime.strptime(due_date_str, '%m/%d/%Y').date()
                except (ValueError, TypeError):
                    return max_date
            return max_date
        sales_orders.sort(key=get_sort_date)

        # 5. Initialize component inventory and allocation log
        live_component_inventory = {
            part.strip(): data.get('approved', 0) for part, data in component_inventory.items()
        }
        allocation_log = {}

        print(f"MRP RUN: Sorted {len(sales_orders)} SO lines. Starting allocation...")

        # 6. Process each sales order sequentially
        mrp_results = []
        for so in sales_orders:
            part_number = so['Part'].strip()
            so_number = str(so['SO']) 
            ord_qty_curr_level = so.get('Ord Qty - Cur. Level', 0)

            fg_inv_static = fg_inventory_map.get(part_number, {'approved': 0, 'pending_qc': 0})
            so['On Hand Qty Approved'] = fg_inv_static.get('approved', 0)
            so['On Hand Qty Pending QC'] = fg_inv_static.get('pending_qc', 0)
            
            is_job_created = False
            job_details_for_so = None
            bottleneck_text_for_job = None
            
            if so_number in jobs_by_so:
                is_job_created = True
                jobs = jobs_by_so[so_number]
                job_details_for_so = jobs
                if len(jobs) == 1:
                    job = jobs[0]
                    bottleneck_text_for_job = f"Job: {job['jo_jobnum']} ({job.get('completed_quantity', 0):,.0f}/{job.get('job_quantity', 0):,.0f})"
                else:
                    job_numbers = ', '.join([str(j['jo_jobnum']) for j in jobs])
                    bottleneck_text_for_job = f"Jobs: {job_numbers}"

            needed = ord_qty_curr_level
            
            available_approved = live_fg_approved.get(part_number, 0)
            fulfilled_from_approved = min(needed, available_approved)
            
            if part_number in live_fg_approved:
                live_fg_approved[part_number] -= fulfilled_from_approved
            
            needed -= fulfilled_from_approved

            so['Net Qty'] = needed if needed > 0 else 0

            if needed <= 0:
                mrp_results.append({
                    'sales_order': so, 'components': [], 'bottleneck': 'None', 
                    'can_produce_qty': ord_qty_curr_level, 'status': 'ready-to-ship', 
                    'shifts_required': 0, 'shippable_qty': fulfilled_from_approved, 'producible_qty': 0,
                    'material_status': 'ready-to-ship'
                })
                continue

            available_qc = live_fg_qc.get(part_number, 0)
            if needed <= available_qc:
                if part_number in live_fg_qc:
                    live_fg_qc[part_number] -= needed
                
                status = 'pending-qc'
                bottleneck_text = f"Pending QC Hold: {so['On Hand Qty Pending QC']:,.0f}"

                mrp_results.append({
                    'sales_order': so, 'components': [], 'bottleneck': bottleneck_text, 
                    'can_produce_qty': fulfilled_from_approved, 'status': status, 
                    'shifts_required': 0, 'shippable_qty': fulfilled_from_approved, 'producible_qty': 0,
                    'material_status': 'pending-qc'
                })
                continue

            net_production_qty = needed
            
            final_can_produce_qty = float('inf')
            bom_components = boms_by_parent.get(part_number, [])
            bottleneck_parts = []
            
            if not bom_components:
                final_can_produce_qty = 0
                bottleneck = "No BOM Found"
                prod_status = 'critical'
            else:
                component_build_calcs = []
                for component in bom_components:
                    comp_part_num = component['Part Number'].strip()
                    qty_per_unit = component['Quantity'] * (1 + (component.get('Scrap %', 0) / 100))
                    if qty_per_unit <= 0: continue

                    initial_inv = component_inventory.get(comp_part_num, {'approved': 0, 'pending_qc': 0})
                    inventory_before_this_so = live_component_inventory.get(comp_part_num, 0)
                    pending_qc_qty = initial_inv.get('pending_qc', 0)
                    available_for_build = inventory_before_this_so + pending_qc_qty
                    max_build_for_comp = available_for_build / qty_per_unit
                    
                    component_build_calcs.append({'part': comp_part_num, 'max_build': max_build_for_comp})
                    final_can_produce_qty = min(final_can_produce_qty, max_build_for_comp)

                final_can_produce_qty = min(final_can_produce_qty, net_production_qty)

                for calc in component_build_calcs:
                    if calc['max_build'] < net_production_qty:
                        bottleneck_parts.append(calc['part'])

                if final_can_produce_qty >= net_production_qty:
                    prod_status = 'ok'
                    bottleneck = "Full Production Ready - Create job now"
                else:
                    prod_status = 'partial' if final_can_produce_qty > 0 else 'critical'
                    bottleneck = "Material Shortage"

            if fulfilled_from_approved > 0:
                prod_status = 'partial-ship'

            component_details = []
            if bom_components:
                for component in bom_components:
                    comp_part_num = component['Part Number'].strip()
                    qty_per_unit = component['Quantity'] * (1 + (component.get('Scrap %', 0) / 100))
                    if qty_per_unit <= 0: continue
                    
                    initial_inv = component_inventory.get(comp_part_num, {'approved': 0, 'pending_qc': 0})
                    inventory_before_this_so = live_component_inventory.get(comp_part_num, 0)
                    open_po_qty = pos_by_part.get(comp_part_num, 0)

                    required_for_constrained_build = final_can_produce_qty * qty_per_unit
                    allocated_for_this_so = min(inventory_before_this_so, required_for_constrained_build)
                    if comp_part_num in live_component_inventory:
                        live_component_inventory[comp_part_num] -= allocated_for_this_so

                    if comp_part_num not in allocation_log:
                        allocation_log[comp_part_num] = []
                    if allocated_for_this_so > 0:
                        allocation_log[comp_part_num].append({ 'so': so['SO'], 'allocated': allocated_for_this_so })

                    total_original_need = net_production_qty * qty_per_unit
                    available_for_allocation_with_po = inventory_before_this_so + initial_inv.get('pending_qc', 0) + open_po_qty
                    shortfall = max(0, total_original_need - available_for_allocation_with_po)

                    shared_with_so_details = []
                    total_allocated_to_others = 0
                    if comp_part_num in allocation_log:
                        for allocation in allocation_log[comp_part_num]:
                            if allocation['so'] != so['SO']:
                                total_allocated_to_others += allocation['allocated']
                        if total_allocated_to_others > 0:
                            shared_with_so_details.insert(0, f"Total Allocated to Prior SOs: {total_allocated_to_others:,.2f}")
                            for allocation in allocation_log[comp_part_num]:
                                if allocation['so'] != so['SO']:
                                    shared_with_so_details.append(f"  - SO {allocation['so']}: {allocation['allocated']:,.2f}")

                    component_details.append({
                        'part_number': comp_part_num, 'description': component['Description'],
                        'shared_with_so': shared_with_so_details, 'total_required': ord_qty_curr_level * qty_per_unit,
                        'on_hand_initial': initial_inv['approved'], 'inventory_before_this_so': inventory_before_this_so,
                        'allocated_for_this_so': allocated_for_this_so, 'open_po_qty': open_po_qty,
                        'shortfall': shortfall
                    })
            
            if 'No BOM Found' not in bottleneck:
                if prod_status == 'partial':
                    producible_formatted = f"{final_can_produce_qty:,.0f}"
                    bottleneck = f"Partial Production Ready - Producible: {producible_formatted} - {', '.join(bottleneck_parts)}"
                elif prod_status == 'critical':
                    bottleneck = f"Critical Shortage - {', '.join(bottleneck_parts)}"

            if is_job_created:
                bottleneck = f"{bottleneck_text_for_job} - {', '.join(bottleneck_parts)}" if bottleneck_parts else bottleneck_text_for_job
            
            if prod_status == 'partial-ship':
                 bottleneck = f"Partial Ship: {fulfilled_from_approved:,.0f} / Prod. Needed: {net_production_qty:,.0f} / Producible: {final_can_produce_qty:,.0f}"

            final_status = 'job-created' if is_job_created else prod_status

            so_result = {
                'sales_order': so, 
                'components': component_details, 
                'bottleneck': bottleneck,
                'bottleneck_parts': bottleneck_parts,
                'can_produce_qty': fulfilled_from_approved + final_can_produce_qty,
                'shifts_required': 0, 
                'status': final_status,
                'material_status': prod_status,
                'job_details': job_details_for_so,
                'shippable_qty': fulfilled_from_approved,
                'producible_qty': final_can_produce_qty
            }

            if capacities:
                line_capacity = next(iter(capacities.values()), 0)
                if line_capacity > 0:
                    so_result['shifts_required'] = (net_production_qty / line_capacity) if line_capacity > 0 else 0

            mrp_results.append(so_result)

        mrp_results.sort(key=lambda r: r['sales_order']['SO'])
        print("MRP RUN: Calculation complete.")
        return mrp_results

    def get_customer_summary(self, customer_orders):
        """
        Summarizes a pre-filtered list of MRP results for a specific customer.
        Returns richer data for a more comprehensive view.
        """
        if not customer_orders:
            return None

        summary = {
            'total_open_orders': len(customer_orders),
            'on_track_orders': 0,
            'at_risk_orders': 0,
            'critical_orders': 0,
            'orders': []
        }

        for result in customer_orders:
            # Find only the components that are causing a shortfall for display
            bottleneck_components = [
                comp for comp in result.get('components', []) if comp.get('shortfall', 0) > 0
            ]
            result['bottleneck_components'] = bottleneck_components
            has_bottlenecks = len(bottleneck_components) > 0

            # Determine a simplified summary status with improved logic
            if result['status'] == 'critical':
                result['summary_status'] = 'Critical'
                summary['critical_orders'] += 1
            elif result['status'] == 'job-created' and has_bottlenecks:
                result['summary_status'] = 'At-Risk'
                summary['at_risk_orders'] += 1
            elif result['status'] in ['partial', 'partial-ship', 'pending-qc']:
                result['summary_status'] = 'At-Risk'
                summary['at_risk_orders'] += 1
            else:  # 'ok', 'ready-to-ship', and 'job-created' without bottlenecks
                result['summary_status'] = 'On-Track'
                summary['on_track_orders'] += 1
            
            summary['orders'].append(result)

        return summary

    def get_consolidated_shortages(self):
        """
        Runs the full MRP calculation and then consolidates all component
        shortages into a single list, ideal for purchasing.
        """
        mrp_results = self.calculate_mrp_suggestions()
        
        shortages = {}
        all_customers = set()
        far_future_date = datetime.strptime('12/31/2999', '%m/%d/%Y').date()

        for result in mrp_results:
            so_info = result['sales_order']
            customer = so_info.get('Customer Name', 'N/A')
            if customer != 'N/A':
                all_customers.add(customer)

            for component in result.get('components', []):
                if component.get('shortfall', 0) > 0:
                    part_number = component['part_number']

                    # Filter out parts starting with 'W'
                    if part_number.startswith('W'):
                        continue

                    if part_number not in shortages:
                        shortages[part_number] = {
                            'part_number': part_number,
                            'description': component['description'],
                            'on_hand': component['on_hand_initial'],
                            'open_po_qty': component['open_po_qty'],
                            'total_shortfall': 0,
                            'affected_orders': [],
                            'affected_customers': set(),
                            'earliest_due_date': far_future_date
                        }
                    
                    shortages[part_number]['total_shortfall'] += component['shortfall']
                    
                    due_date_str = so_info.get('Due to Ship')
                    current_due_date = None
                    if due_date_str:
                        try:
                            current_due_date = datetime.strptime(due_date_str, '%m/%d/%Y').date()
                        except (ValueError, TypeError):
                            pass

                    shortages[part_number]['affected_orders'].append({
                        'so': so_info['SO'],
                        'customer': customer,
                        'due_date': due_date_str
                    })
                    if customer != 'N/A':
                        shortages[part_number]['affected_customers'].add(customer)
                    
                    # Update the earliest due date
                    if current_due_date and current_due_date < shortages[part_number]['earliest_due_date']:
                        shortages[part_number]['earliest_due_date'] = current_due_date

        # Convert the dictionary to a sorted list
        sorted_shortages = sorted(shortages.values(), key=lambda x: x['part_number'])
        
        # Format sets and dates for the template
        for item in sorted_shortages:
            item['affected_customers'] = sorted(list(item['affected_customers']))
            if item['earliest_due_date'] == far_future_date:
                item['earliest_due_date'] = None
            else:
                item['earliest_due_date'] = item['earliest_due_date'].strftime('%m/%d/%Y')

        return {
            'shortages': sorted_shortages,
            'customers': sorted(list(all_customers))
        }


# Singleton instance
mrp_service = MRPService()