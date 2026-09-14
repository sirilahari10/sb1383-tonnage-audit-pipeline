


#  SB 1383 Tonnage Compliance & Audit Pipeline

In environmental compliance reporting for state regulators (like CalRecycle), the biggest data engineering challenges are schema drift from third-party haulers and maintaining an unbreakable audit trail. 

This Proof of Work demonstrates an ETL pipeline designed for SMART Compliance's exact use case. It ingests messy quarterly spreadsheet data, standardizes drifting schemas, and prepares it for a PostgreSQL backend.

## Key Features Built for this PoW:
1. **Schema Drift Adapter:** Automatically maps inconsistent hauler column names (e.g., `Vendor_Name` vs `Hauler`) into a standard internal schema.
2. **Entity Resolution:** Cleans and resolves fragmented hauler names (e.g., `WM Inc.` vs `Waste Mgmt`) into a canonical Golden Record.
3. **Declarative Validation (Pandera):** Enforces strict data types and constraints (e.g., tonnage cannot be negative) before the data ever reaches the database.
4. **JSONB Audit Trail:** Preserves the exact raw input row as a JSON string alongside the clean data, ensuring that every reported number can be traced back to its raw source for state audits.

## Run Locally
```bash
pip install -r requirements.txt
python etl_pipeline.py
