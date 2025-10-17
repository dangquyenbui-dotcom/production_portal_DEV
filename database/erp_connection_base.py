# database/erp_connection_base.py
"""
Dedicated ERP Database Connection Base.
Handles the raw pyodbc connection logic.
"""
import pyodbc
import traceback
from config import Config

class ERPConnection:
    """Handles the raw connection to the ERP database."""
    def __init__(self):
        self.connection = None
        self._connection_string = None # Store the successful connection string

        # Prioritized list of potential drivers to try (braces removed)
        drivers_to_try = [
            Config.ERP_DB_DRIVER,  # First, try the one from .env
            'ODBC Driver 18 for SQL Server',
            'ODBC Driver 17 for SQL Server',
            'SQL Server Native Client 11.0',
            'SQL Server'
        ]

        # Remove duplicates while preserving order
        drivers = list(dict.fromkeys(d for d in drivers_to_try if d)) # Ensure no None or empty strings

        for driver in drivers:
            try:
                # The f-string correctly adds the necessary braces around the driver name
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={Config.ERP_DB_SERVER},{Config.ERP_DB_PORT};"
                    f"DATABASE={Config.ERP_DB_NAME};"
                    f"UID={Config.ERP_DB_USERNAME};"
                    f"PWD={Config.ERP_DB_PASSWORD};"
                    f"TrustServerCertificate=yes;"
                    f"Connection Timeout={Config.ERP_DB_TIMEOUT};"
                )
                self.connection = pyodbc.connect(connection_string, autocommit=True)
                print(f"✅ [ERP_DB] Connection successful using driver: {driver}")
                self._connection_string = connection_string  # Save the working string
                break  # Exit loop on successful connection
            except pyodbc.Error as e:
                # Only print error if it's not a driver-related issue that we expect to retry
                if 'driver' not in str(e).lower():
                    print(f"❌ [ERP_DB] Connection Error: {e}")
                print(f"ℹ️  [ERP_DB] Driver '{driver}' failed. Trying next...")
                continue # Try the next driver in the list

        if not self.connection:
            print(f"❌ [ERP_DB] FATAL: Connection failed. All attempted drivers were unsuccessful.")
            # Optionally, raise an exception here if a connection is critical
            # raise ConnectionError("Could not establish connection to ERP database.")


    def execute_query(self, sql, params=None):
        """Executes a SQL query and returns results as a list of dicts."""
        if not self.connection:
            print("❌ [ERP_DB] Cannot execute query, no active connection.")
            return []

        try:
            # Check if connection is still valid
            # A simple check like this might not always detect a stale connection.
            # Depending on the driver and DB, a `SELECT 1` might be needed.
            if self.connection.closed:
                 print("❌ [ERP_DB] Connection is closed. Cannot execute query.")
                 return []

            cursor = self.connection.cursor()
            cursor.execute(sql, params or [])
            if cursor.description:
                columns = [column[0] for column in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                cursor.close()
                return results
            cursor.close()
            # Handle cases like INSERT/UPDATE/DELETE where description might be None
            # If autocommit=True, changes are already committed.
            return [] # Return empty list for non-SELECT or empty results
        except pyodbc.Error as e:
            print(f"❌ [ERP_DB] Query Failed: {e}")
            print(f"   SQL: {sql}")
            print(f"   Params: {params}")
            traceback.print_exc()
            # Attempt to reconnect might be added here, but be careful of loops.
            return []
        except Exception as e:
            print(f"❌ [ERP_DB] Unexpected error during query execution: {e}")
            traceback.print_exc()
            return []

    def close(self):
        """Closes the database connection."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                print("ℹ️ [ERP_DB] Connection closed.")
            except pyodbc.Error as e:
                print(f"⚠️ [ERP_DB] Error closing connection: {e}")


# Singleton instance for the connection (optional, consider lifetime management)
_erp_connection_instance = None

def get_erp_db_connection():
    """
    Gets a shared instance of the ERP connection.
    Creates a new one if it doesn't exist or seems closed.
    """
    global _erp_connection_instance
    # Check if instance exists and if the connection is still open (basic check)
    if _erp_connection_instance is None or getattr(_erp_connection_instance.connection, 'closed', True):
        print("ℹ️ [ERP_DB] Creating new ERPConnection instance.")
        _erp_connection_instance = ERPConnection()
        if _erp_connection_instance.connection is None:
             # Handle the case where the connection failed during initialization
             print("❌ [ERP_DB] Failed to create a valid ERPConnection.")
             return None # Or raise an exception
    return _erp_connection_instance