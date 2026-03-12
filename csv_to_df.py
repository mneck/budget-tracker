import glob
import os
import pandas as pd

# ---------- helpers ---------------------------------------------------

def load_all_transactions(data_dir="data", exclude=None):
    """Read every CSV in *data_dir* except the ones named in *exclude*.

    Returns a single combined `DataFrame` with an extra ``source_file``
    column so you can trace where each row came from.
    """
    if exclude is None:
        exclude = []

    pattern = os.path.join(data_dir, "*.csv")
    files = [f for f in glob.glob(pattern) if os.path.basename(f) not in exclude]

    dfs = []
    for f in files:
        dfs.append(pd.read_csv(f).assign(source_file=os.path.basename(f)))
    return pd.concat(dfs, ignore_index=True)


# ---------- load lookup ------------------------------------------------

vendors = pd.read_csv("data/vendors.csv")
# vendors.csv looks like:
#    Vendor Name,Type
#    GASAG,Utilities
#    ...

# ---------- build transactions dataframe ------------------------------

transactions = load_all_transactions(exclude=["vendors.csv"])

# merge the vendor type onto each transaction; non‑matched names will
# receive NaN for ``Type`` which we replace with ``Unknown`` below.
merged = (
    transactions
    .merge(vendors, left_on="Partner Name", right_on="Vendor Name", how="left")
)
merged["Type"] = merged["Type"].fillna("Unknown")

# ---------- examples of what you can do -------------------------------

# total amount by vendor type
print("Totals by vendor type:")
print(merged.groupby("Type")["Amount (EUR)"].sum().round(2))

# save a copy with the category attached for later analysis
merged.to_csv("data/all_transactions_categorized.csv", index=False)

# you can also inspect a single month, filter, plot, etc.
# e.g.:
# print(merged[merged.source_file.str.contains("Dec")].head())