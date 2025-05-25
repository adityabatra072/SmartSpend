import os
import pandas as pd

# Keywords used to identify transaction tables
REQUIRED_KEYWORDS = ['date', 'description', 'remarks', 'narration', 'particulars', 'debit', 'credit', 'amount']

def is_transaction_table(df):
    """Heuristic check for transaction table structure."""
    cols = [str(col).lower() for col in df.columns]

    # Must contain at least one date-like column
    has_date = any("date" in col for col in cols)
    has_amount = any("amount" in col or "debit" in col or "credit" in col for col in cols)
    has_desc = any("description" in col or "narration" in col or "remarks" in col or "particulars" in col for col in cols)

    score = sum(1 for kw in REQUIRED_KEYWORDS if any(kw in col for col in cols))
    return has_date and (has_amount or has_desc) and score >= 2 and len(df.columns) >= 3


def normalize_columns(df):
    """Map different bank terms to standard column names."""
    rename_map = {}
    for col in df.columns:
        col_lower = str(col).lower()
        if "date" in col_lower:
            rename_map[col] = "Date"
        elif any(x in col_lower for x in ["description", "remarks", "narration", "particulars"]):
            rename_map[col] = "Description"
        elif "amount" in col_lower and "withdrawal" not in col_lower and "deposit" not in col_lower:
            rename_map[col] = "Amount"
        elif "withdrawal" in col_lower or "debit" in col_lower:
            rename_map[col] = "Debit"
        elif "deposit" in col_lower or "credit" in col_lower:
            rename_map[col] = "Credit"
        elif "debit / credit" in col_lower or "dr/cr" in col_lower:
            rename_map[col] = "Type"
    return df.rename(columns=rename_map)



def compute_signed_amount(df):
    """Create unified signed 'Amount' column."""

    # Case 1: Already has 'Amount' column
    if "Amount" in df.columns:
        df["Amount"] = pd.to_numeric(df["Amount"], errors='coerce')

        if "Type" in df.columns:
            df["Type"] = df["Type"].astype(str).str.strip().str.lower()
            df["Amount"] = df.apply(
                lambda row: -row["Amount"] if "debit" in row["Type"] else row["Amount"],
                axis=1
            )

    # Case 2: Has separate Debit/Credit or Withdrawal/Deposit columns
    else:
        possible_debit = ["debit", "withdrawal", "withdrawal amt.", "withdrawal amount"]
        possible_credit = ["credit", "deposit", "deposit amt.", "deposit amount"]

        debit_col = next((col for col in df.columns if str(col).lower().strip() in possible_debit), None)
        credit_col = next((col for col in df.columns if str(col).lower().strip() in possible_credit), None)

        debit = pd.to_numeric(df.get(debit_col, 0), errors='coerce').fillna(0)
        credit = pd.to_numeric(df.get(credit_col, 0), errors='coerce').fillna(0)

        df["Amount"] = credit - debit

    return df


def clean_transactions(df):
    """Clean and standardize transaction data, remove gibberish and footer rows, assign debit/credit type."""

    # Step 1: Parse and filter dates
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna() & (df["Date"] >= pd.Timestamp("1985-01-01"))]
    df["Date"] = df["Date"].dt.strftime('%Y-%m-%d')

    # Step 2: Clean Description
    df["Description"] = df["Description"].astype(str).str.strip()

    # Step 3: Clean Amount
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    df = df.dropna(subset=["Amount"])

    # Step 4: Drop ghost/gibberish lines
    df = df[
        (~df["Description"].str.lower().isin(["nan", "", "none", "null"])) &
        (~df["Description"].str.match(r"^\d+$")) &
        (df["Description"].str.len() > 2)
    ]

    # Step 5: Remove footers
    footer_keywords = ["total", "closing", "balance", "summary", "opening"]
    cutoff_index = len(df)
    for i, desc in enumerate(df["Description"].iloc[::-1]):
        if any(kw in desc.lower() for kw in footer_keywords):
            cutoff_index = len(df) - i - 1
            break
    df = df.iloc[:cutoff_index].reset_index(drop=True)

    # ✅ Step 6: Add 'Type' and convert 'Amount' to positive
    df["Type"] = df["Amount"].apply(lambda x: "Debit" if x < 0 else "Credit")
    df["Amount"] = df["Amount"].abs()

    return df[["Date", "Description", "Amount", "Type"]]





def find_transaction_table(sheet_df, sheet_name="Unknown"):
    max_search_rows = min(100, len(sheet_df))

    for i in range(max_search_rows):
        header_row = sheet_df.iloc[i].values
        # Convert all header values to string, strip and lower
        header = [str(x).strip() if pd.notna(x) else '' for x in header_row]

        # Skip header if too many blanks or numbers
        non_empty = sum(1 for h in header if h)
        if non_empty < 3:
            continue

        candidate = sheet_df.iloc[i+1:].copy()
        candidate.columns = header
        if is_transaction_table(candidate):
            print(f"✅ Found transaction table in sheet '{sheet_name}' starting at row {i}")
            print("📌 Detected header:", header)
            return candidate

    print(f"⚠️ No transaction table found in sheet '{sheet_name}'")
    return None



def load_and_parse_statement(file_path):
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".csv":
        df = pd.read_csv(file_path, header=None)
        df = find_transaction_table(df, "CSV")
        if df is not None:
            df = normalize_columns(df)
            df = compute_signed_amount(df)
            return clean_transactions(df)
        else:
            raise ValueError("No valid transaction table found in CSV.")

    elif ext in [".xlsx", ".xls"]:
        xls = pd.ExcelFile(file_path)
        for sheet in xls.sheet_names:
            sheet_df = xls.parse(sheet, header=None)
            df = find_transaction_table(sheet_df, sheet)
            if df is not None:
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
cleaned_file = parse_and_save("SampleData/ss2.xls")