import os
import pandas as pd
from datetime import datetime

# ---------- CONFIG ----------
input_path = "online_retail.csv"   # path to your CSV (with columns: Invoice, StockCode, Description, Quantity, InvoiceDate, Price, Customer, Country)
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# ---------- STEP 0: Load CSV ----------
df = pd.read_csv(input_path, dtype={'Invoice': str}, parse_dates=['InvoiceDate'], dayfirst=True, low_memory=False)
print(f"✅ Loaded dataset ({len(df)} rows)")

# Normalize column names
df.columns = df.columns.str.strip().str.lower()
print("📋 Columns found:", df.columns.tolist())

# ---------- STEP 1: Prepare transactions.csv ----------
# Your dataset columns now are:
# invoice, stockcode, description, quantity, invoicedate, price, customer, country

# Ensure numeric conversion
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
df['price'] = pd.to_numeric(df['price'], errors='coerce')

# Compute amount = quantity * price
df['amount'] = df['quantity'] * df['price']

# Generate unique transaction id (invoice + row index)
df = df.reset_index(drop=False)
df['txn_id'] = df['invoice'].astype(str) + "-" + df['index'].astype(str)

# Normalize customer_id
df['customer_id'] = df['customer id'].apply(lambda x: str(int(x)) if pd.notnull(x) else None)

# Rename date column
df['txn_date'] = df['invoicedate']

# Select final columns for transactions.csv
transactions = df[['txn_id', 'customer_id', 'amount', 'txn_date', 'invoice', 'stockcode', 'description', 'country']]
transactions.to_csv(os.path.join(output_dir, "transactions.csv"), index=False)
print(f"✅ transactions.csv saved ({len(transactions)} rows)")

# ---------- STEP 2: Create master_records.csv ----------
master = transactions[['customer_id']].dropna().drop_duplicates().reset_index(drop=True)
master['customer_name'] = master['customer_id'].apply(lambda x: f"cust_{x}")
master['account_status'] = "ACTIVE"
master.to_csv(os.path.join(output_dir, "master_records.csv"), index=False)
print(f"✅ master_records.csv saved ({len(master)} rows)")

# ---------- STEP 3: Create reconciliation_rules.csv ----------
rules_df = pd.DataFrame([
    {"rule_id": "R003", "description": "Customer not found in master", "severity": "blocking"},
    {"rule_id": "R004", "description": "Negative transaction amount", "severity": "high"},
    {"rule_id": "R005", "description": "Unmatched transaction record", "severity": "high"},
])
rules_df.to_csv(os.path.join(output_dir, "reconciliation_rules.csv"), index=False)
print("✅ reconciliation_rules.csv saved")

# ---------- STEP 4: Data Quality Checks ----------
def check_nulls(df, name):
    null_summary = df.isnull().sum().reset_index()
    null_summary.columns = ['column', 'null_count']
    null_summary['table'] = name
    return null_summary

dq_report = pd.concat([
    check_nulls(master, 'master_records'),
    check_nulls(transactions, 'transactions')
], ignore_index=True)

# Check duplicates
dup_master = master[master.duplicated(subset=['customer_id'], keep=False)]
dup_txn = transactions[transactions.duplicated(subset=['txn_id'], keep=False)]

dq_report['timestamp'] = datetime.now()
dq_report.to_csv(os.path.join(output_dir, "data_quality_report.csv"), index=False)
print("📊 Data Quality Report saved")

# ---------- STEP 5: Rule-based Validation ----------
exceptions = []

# R003 - Customer must exist in master
unknown_customers = transactions[
    (transactions['customer_id'].notna()) &
    (~transactions['customer_id'].isin(master['customer_id']))
]
for _, row in unknown_customers.iterrows():
    exceptions.append({
        "rule_id": "R003",
        "description": "Customer not found in master",
        "txn_id": row['txn_id'],
        "customer_id": row['customer_id'],
        "severity": "blocking"
    })

# R004 - Negative amount
negative_txns = transactions[transactions['amount'] < 0]
for _, row in negative_txns.iterrows():
    exceptions.append({
        "rule_id": "R004",
        "description": "Negative transaction amount (return or cancellation)",
        "txn_id": row['txn_id'],
        "customer_id": row['customer_id'],
        "severity": "high"
    })

# ---------- STEP 6: Reconciliation ----------
reconciled = transactions.merge(master, on='customer_id', how='left', indicator=True)
reconciled['match_status'] = reconciled['_merge'].map({
    'both': 'MATCHED',
    'left_only': 'UNMATCHED'
})
reconciled.drop(columns=['_merge'], inplace=True)

# Add R005 for unmatched
for _, row in reconciled[reconciled['match_status'] == 'UNMATCHED'].iterrows():
    exceptions.append({
        "rule_id": "R005",
        "description": "Unmatched transaction record",
        "txn_id": row['txn_id'],
        "customer_id": row['customer_id'],
        "severity": "high"
    })

# ---------- STEP 7: Save outputs ----------
reconciled.to_csv(os.path.join(output_dir, "reconciliation_results.csv"), index=False)
exceptions_df = pd.DataFrame(exceptions)
exceptions_df.to_csv(os.path.join(output_dir, "exceptions_report.csv"), index=False)

print("✅ Reconciliation complete")
print("📁 Outputs:")
print("- output/data_quality_report.csv")
print("- output/reconciliation_results.csv")
print("- output/exceptions_report.csv")

# ---------- STEP 8: Summary ----------
print("\n--- SUMMARY ---")
print("Total Transactions:", len(transactions))
print("Matched:", len(reconciled[reconciled['match_status'] == 'MATCHED']))
print("Unmatched:", len(reconciled[reconciled['match_status'] == 'UNMATCHED']))
print("Exceptions:", len(exceptions))
# ---------- STEP 7: Beautiful Summary Display in VS Code ----------
from rich.console import Console
from rich.table import Table

console = Console()

# Create and print a summary table
summary = Table(title="📊 Reconciliation Summary", show_lines=True)
summary.add_column("Metric", style="bold cyan")
summary.add_column("Value", justify="right", style="bold yellow")

summary.add_row("Total Transactions", str(len(transactions)))
summary.add_row("Matched", str((reconciled['match_status'] == 'MATCHED').sum()))
summary.add_row("Unmatched", str((reconciled['match_status'] == 'UNMATCHED').sum()))
summary.add_row("Total Exceptions", str(len(exceptions_df)))

console.print(summary)

# Show a few reconciled rows nicely
sample_recon = Table(title="✅ Sample Reconciliation Results", show_lines=False)
for col in reconciled.columns[:8]:  # show first few columns
    sample_recon.add_column(col, overflow="fold")

for _, row in reconciled.head(8).iterrows():
    sample_recon.add_row(*[str(row[c]) for c in reconciled.columns[:8]])

console.print(sample_recon)

# Show a few exception rows
sample_exc = Table(title="⚠️ Sample Exceptions", show_lines=False)
for col in exceptions_df.columns[:6]:
    sample_exc.add_column(col, overflow="fold")

for _, row in exceptions_df.head(8).iterrows():
    sample_exc.add_row(*[str(row[c]) for c in exceptions_df.columns[:6]])

console.print(sample_exc)

console.print("[green]✅ Display complete — scroll up to view tables![/green]")
