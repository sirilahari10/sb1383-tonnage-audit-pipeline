```python
import pandas as pd
import pandera as pa
import json
import re

print("🚀 Starting SB 1383 Compliance ETL Pipeline...\n")

# 1.Messy Hauler Data with Schema Drift
# Q1 has standard columns. Q2 has completely different column names (Schema Drift).
q1_raw_data = [
    {"Hauler": "Waste Mgmt", "Material": "Organics", "Weight_Tons": 145.2, "Date": "2026-03-01"},
    {"Hauler": "Recology SF", "Material": "Mixed Recycling", "Weight_Tons": 89.5, "Date": "2026-03-15"}
]

q2_raw_data = [
    # Notice the column names changed: Vendor_Name, Waste_Type, Tonnage_Vol
    {"Vendor_Name": "WM Inc.", "Waste_Type": "Organics", "Tonnage_Vol": 150.0, "Date": "2026-06-01"},
    {"Vendor_Name": "Republic Services", "Waste_Type": "Solid Waste", "Tonnage_Vol": -5.0, "Date": "2026-06-10"} # Deliberate error: negative tonnage
]

df_q1 = pd.DataFrame(q1_raw_data)
df_q2 = pd.DataFrame(q2_raw_data)

# 2. SCHEMA DRIFT ADAPTER
# Maps unpredictable customer columns to our strict internal database schema
schema_mapping = {
    "Vendor_Name": "hauler_name",
    "Hauler": "hauler_name",
    "Material": "material_type",
    "Waste_Type": "material_type",
    "Weight_Tons": "tonnage",
    "Tonnage_Vol": "tonnage",
    "Date": "collection_date"
}

def standardize_columns(df):
    """Renames drifting columns to our canonical internal schema."""
    #capture the raw payload for the audit trail (Simulating PostgreSQL JSONB)
    df['raw_source_record_jsonb'] = df.apply(lambda row: json.dumps(row.to_dict()), axis=1)
    
    # Rename columns based on the adapter mapping
    return df.rename(columns=schema_mapping)

df_q1_standard = standardize_columns(df_q1)
df_q2_standard = standardize_columns(df_q2)
combined_df = pd.concat([df_q1_standard, df_q2_standard], ignore_index=True)

print("✅ Schema Drift Handled. Combined Data:")
print(combined_df[['hauler_name', 'tonnage', 'raw_source_record_jsonb']].head(2).to_string(), "\n")

# 3. ENTITY RESOLUTION
# Standardizing hauler names for accurate state reporting
def resolve_entity(name):
    name = str(name).lower()
    name = re.sub(r'\b(inc|llc|corp|\.|,)\b', '', name).strip()
    if 'wm' in name or 'waste mgmt' in name or 'waste management' in name:
        return 'Waste Management'
    return name.title()

combined_df['canonical_hauler_id'] = combined_df['hauler_name'].apply(resolve_entity)

# 4. DECLARATIVE VALIDATION (using Pandera)
compliance_schema = pa.DataFrameSchema({
    "canonical_hauler_id": pa.Column(str, nullable=False),
    "material_type": pa.Column(str, nullable=False),
    "tonnage": pa.Column(float, checks=pa.Check.ge(0, error="Tonnage cannot be negative for state reporting")),
    "collection_date": pa.Column(pa.DateTime, nullable=False),
    "raw_source_record_jsonb": pa.Column(str, nullable=False) # The audit trail must exist
})

# 5. EXECUTE PIPELINE & CATCH ERRORS
print("⏳ Running Pandera Validation Engine...\n")
combined_df['collection_date'] = pd.to_datetime(combined_df['collection_date'])

try:
    validated_df = compliance_schema.validate(combined_df, lazy=True)
    print("✅ All data passed compliance validation.")
except pa.errors.SchemaErrors as err:
    print("❌ VALIDATION FAILED (Catching bad data before it hits PostgreSQL):")
    print(err.failure_cases[['column', 'check', 'failure_case']].to_string())
    
    # Dropping the bad rows
    clean_indices = ~combined_df.index.isin(err.failure_cases['index'])
    validated_df = combined_df[clean_indices]
