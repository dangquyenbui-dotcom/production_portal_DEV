Okay, here is the updated full content for your `README.md` file, incorporating the recent changes including the Certificate of Compliance report and its specific query logic.

````markdown
# Production Portal: Downtime Tracker & MRP Scheduler

## Overview

A robust, enterprise-ready web application designed for manufacturing and co-packaging environments. This portal provides a comprehensive suite of tools for **tracking production downtime**, managing **production scheduling**, viewing critical ERP data like **Bills of Materials (BOM)** and **Purchase Orders (PO)**, generating reports like the **Certificate of Compliance (CoC)**, and leveraging a powerful **Material Requirements Planning (MRP)** dashboard to guide production decisions.

The system's hybrid data architecture connects to a **read-only ERP database** for live production and material data while storing all user-generated data—such as downtime events, scheduling projections, and production capacity—in a separate, fully-controlled local SQL Server database (`ProductionDB`).

**Current Version:** 2.6.1 (Added CoC Report)
**Status:** **All core modules are complete and operational.**

-----

## 🚀 Getting Started

### Prerequisites

  * Python 3.10+
  * Microsoft SQL Server
  * Access to an Active Directory domain (for production authentication)

### Installation & Setup

1.  **Clone the Repository:**

    ```bash
    git clone <your-repository-url>
    cd production_portal_dev
    ```

2.  **Set Up Environment Variables:**
    Create a file named `.env` in the root of the project and populate it with your environment-specific configurations. A template of required variables can be found in `config.py`.

3.  **Install Dependencies:**
    It is highly recommended to use a virtual environment.

    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # On Windows
    # source venv/bin/activate  # On macOS/Linux

    pip install -r requirements.txt
    ```

4.  **Run the Application:**
    Execute the main application file. The server will start in debug mode using standard HTTP.

    ```bash
    python app.py
    ```

5.  **Access the URL:**
    Open your browser and navigate to **`http://localhost:5000`** or the network URL provided in the terminal (e.g., `http://192.168.x.x:5000`).

-----

## 🎯 Core Modules

### ✅ Material Requirements Planning (MRP) Dashboard

A dynamic, filterable dashboard that serves as the central planning tool. It analyzes all open sales orders, provides intelligent production suggestions based on material availability, and prioritizes orders by their "Due to Ship" date.

#### Core Logic & Business Rules:

  * **Sequential Allocation by Date:** The MRP engine first sorts all open Sales Orders by their "Due to Ship" date, from earliest to latest. It then processes them one by one, allocating available inventory to the highest-priority orders first.
  * **Finished Goods Allocation:** The system maintains a "live" in-memory inventory of finished goods. As it processes SOs, it depletes this on-hand stock sequentially. An SO is only marked "Ready to Ship" if the live inventory can fully cover its requirement at that moment.
  * **Inventory Availability:** The "Can Produce" calculation for items that require production is based on the sum of three key component inventory figures:
    1.  **Approved, On-Hand Inventory:** The main pool of unrestricted materials.
    2.  **Pending QC Inventory:** Materials that have been received but are awaiting quality inspection. These are included for planning purposes to provide a more realistic view of upcoming availability.
    3.  **Open Purchase Order Quantity:** Materials that are on order but not yet received.
  * **Two-Pass Calculation per Sales Order:** To ensure accuracy for production orders, the engine uses a two-pass system:
    1.  **Pass 1 (Discovery):** It first loops through all required components to find the single greatest constraint (the "bottleneck") and determines the absolute maximum quantity of the finished good that can be produced.
    2.  **Pass 2 (Allocation):** With the true "Can Produce" quantity established, it loops through the components a second time, allocating only the precise amount of each material needed from the "live" component inventory. This prevents over-allocation of non-bottleneck materials and frees them up for lower-priority orders.
  * **Committed Inventory Exclusion:** Inventory that has already been "Issued to Job" is considered Work-in-Progress (WIP) and is **excluded** from all MRP calculations to prevent double-promising materials.

#### Features:

  * **Holistic View:** Displays **all** open sales orders for data consistency and validation against the Scheduling page.
  * **Intelligent Filtering:** Instantly narrow down orders by **Business Unit (BU), Customer, Due to Ship (Month/Year), and Production Status**.
  * **Smart Statuses**:
      * **Ready to Ship:** Orders that can be fulfilled entirely from existing finished goods inventory.
      * **Partial Ship / Production Needed:** A hybrid status for orders that can be partially fulfilled from on-hand stock, with the remainder requiring production. The status text dynamically shows both the shippable and needed quantities (e.g., `"Partial Ship: 10,995 / Prod. Needed: 973"`).
      * **Pending QC:** Orders that cannot be shipped from on-hand stock but can be fully covered by stock that is pending quality control.
      * **Full Production Ready:** A status indicating that all necessary components are available to produce the full required quantity. The status text encourages immediate action: `"Full Production Ready - Create job now"`.
      * **Partial Production Ready:** Indicates that some, but not all, of the required quantity can be produced. The status text lists all bottleneck components: `"Partial Production Ready - [Part1, Part2, ...]"`.
      * **Critical Shortage:** Not enough components are available to produce any of the required product.
  * **Enhanced Tooltips:** Hovering over the 🔗 icon next to a component reveals a detailed tooltip showing the **total quantity allocated to prior orders** and a line-by-line breakdown of which specific Sales Orders consumed that inventory. Hovering over the "Required" quantity for a "Partial Ship" order shows a tooltip with the outstanding quantity to be produced.
  * **Excel Export:** Download the currently filtered and sorted view of the MRP data, including all component details, to an XLSX file.

### Future Improvements & Action Items for MRP Logic

While the current MRP logic is a powerful tool for material-based planning, the following enhancements are planned to fully address the complexities of a dynamic co-packaging environment.

#### 1\. Integrate Production Line Capacity and Constraints

  * **Objective:** Move beyond simple material availability to answer the question, "When can this realistically be produced?"
  * **Action Items:**
      * **Utilize Production Capacity Data:** Integrate the `ProductionCapacityDB` into the MRP engine. For each producible order, calculate the required production `shifts_required` by dividing the needed quantity by the line's `capacity_per_shift`. This will provide a time-based estimate for production.
      * **Line-Specific Capabilities:** Add fields to the `ProductionLines` table (e.g., `tooling_type`, `allergen_status`, `line_speed_modifier`). The MRP logic must be updated to filter and select only from valid production lines when determining production feasibility.

#### 2\. Implement Dynamic Prioritization Beyond "Due Date"

  * **Objective:** Introduce a more flexible, business-driven approach to order prioritization that goes beyond a simple first-in, first-out model based on due dates.
  * **Action Items:**
      * **Develop a Priority Score:** Create a weighted scoring system for each sales order. This score will replace the simple "Due to Ship" date as the primary sorting key for the MRP run.
      * **Incorporate Business Factors:** The priority score should be calculated from a combination of new and existing data points, such as:
          * `Customer Priority Level`: A new field to be added to the customer data model.
          * `Order Value`: The total financial value of the sales order.
          * `Urgency`: A calculated field (e.g., `days_until_due`).
          * `Contractual Obligations`: A flag or field to indicate orders with high-penalty late fees.

#### 3\. Introduce Logic for Customer-Supplied Components

  * **Objective:** Accurately model the reality of co-packaging where clients often provide some or all of the raw materials, and distinguish these from company-owned inventory.
  * **Action Items:**
      * **Differentiate Inventory Types:** Modify the ERP queries in `database/erp_connection.py`, specifically `get_raw_material_inventory`, to identify and flag customer-supplied components.
      * **Create New Statuses:** In the `calculate_mrp_suggestions` method, create new statuses to reflect dependency on customer materials. For example, a shortage of a customer-supplied item should result in an "Awaiting Customer Material" status, not "Critical Shortage."

#### 4\. Support for Multi-Level BOMs (Sub-Assemblies)

  * **Objective:** Enable the MRP engine to handle complex products (like kits or display packs) where a component on one BOM is a finished good with its own BOM.
  * **Action Items:**
      * **Implement Recursive BOM Traversal:** Refactor the `calculate_mrp_suggestions` method in `database/mrp_service.py` to be recursive. When the logic encounters a component that is a "Make Item," it must trigger a sub-MRP calculation for that component before proceeding. This ensures that the material requirements of all sub-assemblies are factored into the top-level production plan.

### ✅ Production Scheduling Module

An Excel-like grid that displays all open sales orders from the ERP, allowing planners to input and save financial projections for different risk scenarios.

### ✅ Downtime Tracking Module

A tablet-optimized interface for quick and easy downtime entry on the factory floor, featuring ERP job integration and a real-time list of the day's entries.

### ✅ Live Open Jobs Viewer

A real-time, filterable, and sortable view of all currently open production jobs, showing header information and detailed component transaction summaries (Issued, De-issued, Relieve Job, Yield Cost/Scrap, Yield Loss). Includes automatic refresh capabilities.

### ✅ BOM & PO Viewers

Dedicated, read-only interfaces for viewing and searching **Bills of Materials** and open **Purchase Orders** directly from the ERP, complete with client-side searching and Excel export functionality.

### ✅ Reporting Suite

A collection of analytical reports accessible via a central hub.

  * **Downtime Summary:** Analyze downtime duration by category and production line within a specified date range, with filtering options. Includes charts and raw data export.
  * **Shipment Forecast:** Automated monthly forecast based on MRP results, categorizing orders as "Likely to Ship" or "At-Risk/Partial".
  * **Certificate of Compliance (CoC):** Allows users to input *any* job number (open or closed) and view detailed component usage, including Issued Inventory, De-issue, Relieve Job quantities, and calculated Yield Cost/Scrap and Yield Loss percentages, mimicking the detailed view from the Live Open Jobs page.

### ✅ Admin Panel & System Management

A comprehensive, role-restricted area for managing all aspects of the application.

  * **Facilities, Lines, Categories, Shifts:** Full CRUD (Create, Read, Update, Deactivate) management for all core data.
  * **Production Capacity:** A dedicated interface to define and manage the output capacity (e.g., units per shift) for each production line. This data is a critical input for the MRP engine.
  * **User Management & Audit Log:** Tools to view user activity and a complete history of all changes made within the system.

-----

## 🏗️ Architecture

### Technology Stack

  * **Backend**: Python, Flask
  * **Database**:
      * **Application DB**: Microsoft SQL Server (via `pyodbc`)
      * **ERP Connection**: Read-only connection to ERP database (via `pyodbc`)
  * **Authentication**: Active Directory (via `ldap3`)
  * **Frontend**: Jinja2, HTML, CSS, JavaScript
  * **Internationalization**: Flask-Babel
  * **Excel Export**: `openpyxl`

### Project Structure (Highlights)

````

/production\_portal\_dev/
|
├── app.py                  \# Main application factory
|
├── /database/
│   ├── connection.py       \# Handles local ProductionDB connection
│   ├── erp\_connection\_base.py \# Base class for raw ERP DB connection logic
│   ├── erp\_service.py      \# Service layer coordinating ERP queries
│   ├── /erp\_queries/       \# Modules containing specific ERP SQL queries
│   │   ├── job\_queries.py
│   │   ├── coc\_queries.py  \# \<-- ADDED: Queries specifically for CoC report
│   │   └── ...             \# Other query modules (bom, inventory, po, qc, sales)
│   ├── mrp\_service.py      \# Core MRP calculation engine
│   ├── capacity.py         \# Manages ProductionCapacity table
│   └── ...                 \# Other local database modules (downtimes, users, etc.)
|
├── /routes/
│   ├── main.py
│   ├── mrp.py              \# Routes for the MRP Dashboard page
│   ├── scheduling.py       \# Routes for the Production Scheduling grid
│   ├── bom.py              \# Routes for the BOM Viewer
│   ├── po.py               \# Routes for the PO Viewer
│   ├── reports.py          \# Routes for Downtime Summary, Forecast, CoC reports
│   ├── jobs.py             \# Routes for Live Open Jobs viewer
│   └── /admin/
│       └── ...             \# All administrative routes
|
├── /static/
│   ├── /css/
│   └── /js/
│       ├── mrp.js          \# JavaScript for the MRP Dashboard
│       ├── scheduling.js   \# JavaScript for the Scheduling page
│       └── jobs.js         \# JavaScript for the Open Jobs page
|
├── /templates/
│   ├── dashboard.html  
│   ├── /mrp/
│   │   └── index.html      \# Main MRP Dashboard page template
│   ├── /reports/
│   │   ├── hub.html
│   │   ├── downtime\_summary.html
│   │   └── coc.html        \# \<-- ADDED: Template for CoC report
│   ├── /jobs/
│   │   └── index.html      \# Template for Open Jobs viewer
│   └── ...
|
├── .env                    \# Environment variables (Create this file)
├── requirements.txt
└── ...

```
```