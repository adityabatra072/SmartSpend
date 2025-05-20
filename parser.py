import os
import pandas as pd

# Keywords used to identify transaction tables
REQUIRED_KEYWORDS = ['date', 'description', 'remarks', 'narration', 'particulars', 'debit', 'credit', 'amount']

def is_transaction_table(df):
    """Heuristic check for transaction table structure."""
    cols = [str(col).lower() for col in df.columns]
    score = sum(1 for kw in REQUIRED_KEYWORDS if any(kw in col for col in cols))
    return score >= 2 and len(df.columns) >= 3

def normalize_columns(df):
    """Map different bank terms to standard column names."""
    rename_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if "date" in col_lower:
            rename_map[col] = "Date"
        elif any(x in col_lower for x in ["description", "remarks", "narration", "particulars"]):
            rename_map[col] = "Description"
        elif "amount" in col_lower:
            rename_map[col] = "Amount"
        elif "debit" in col_lower:
            rename_map[col] = "Debit"
        elif "credit" in col_lower:
            rename_map[col] = "Credit"
    return df.rename(columns=rename_map)

def compute_signed_amount(df):
    """Create unified signed 'Amount' column."""
    if "Amount" in df.columns:
        df["Amount"] = pd.to_numeric(df["Amount"], errors='coerce')
    elif "Debit" in df.columns or "Credit" in df.columns:
        debit = pd.to_numeric(df.get("Debit", 0), errors='coerce').fillna(0)
        credit = pd.to_numeric(df.get("Credit", 0), errors='coerce').fillna(0)
        df["Amount"] = credit - debit
    return df

def clean_transactions(df):
    """Final cleaning: standardize Date/Description/Amount."""
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Description"] = df["Description"].astype(str).str.strip()
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df.dropna(subset=["Date", "Amount", "Description"])
    return df[["Date", "Description", "Amount"]].reset_index(drop=True)

def load_and_parse_statement(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path)
        if is_transaction_table(df):
            df = normalize_columns(df)
            df = compute_signed_amount(df)
            return clean_transactions(df)
        else:
            raise ValueError("No valid transaction table found in CSV.")

    elif ext in [".xlsx", ".xls"]:
        xls = pd.ExcelFile(file_path)
        for sheet in xls.sheet_names:
            df = xls.parse(sheet)
            if is_transaction_table(df):
                df = normalize_columns(df)
                df = compute_signed_amount(df)
                return clean_transactions(df)
        raise ValueError("No valid transaction table found in Excel file.")

    else:
        raise ValueError("Unsupported file type. Please upload a .csv or .xlsx/.xls file.")

def parse_and_save(file_path):
    df = load_and_parse_statement(file_path)
    basename = os.path.splitext(os.path.basename(file_path))[0]
    dir_name = os.path.dirname(file_path)
    output_path = os.path.join(dir_name, f"{basename}_cleaned.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved cleaned file to: {output_path}")
    return output_path

# Example usage:
# cleaned_file = parse_and_save("path/to/your/bank_statement.xlsx")
