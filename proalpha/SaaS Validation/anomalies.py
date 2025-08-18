import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("future.no_silent_downcasting", True)


def detect_anomalies(df, tolerance=0.02):
    """
    Detects anomalies in SaaS revenue data for each customer.
    An anomaly is detected if a single month's revenue is significantly different
    from both its previous and next values. If a value appears at least twice in succession,
    it is considered normal and not an anomaly.

    Parameters:
        df (pd.DataFrame): Revenue table with customers as rows and months as columns.

    Returns:
        pd.DataFrame: A table with detected anomalies.
    """
    anomalies = []
    for index, row in df.iterrows():
        values = pd.to_numeric(row.iloc[3:].fillna(0), errors="coerce")

        if len(values) < 3:
            continue  # Skip if not enough data points

        for i in range(6, len(values) - 1):  # Exclude first and last month
            prev = values.iloc[i - 1]
            curr = values.iloc[i]
            next_ = values.iloc[i + 1]

            # Detect an isolated anomaly (not part of a repeating pattern)
            if not (
                abs(curr - prev) / max(prev, 1) <= tolerance
                or abs(curr - next_) / max(next_, 1) <= tolerance
            ):
                anomalies.append(
                    {
                        "Subgroup": row["Subgroup"],
                        "Logo": row["Logo"],
                        "Customer": row["Customer"],
                        "CustomerID": row["CustomerID"],
                        "Revenue Stream (Mgmt)": row["Revenue Stream (Mgmt)"],
                        "LegalEntity": row["LegalEntity"],
                        "LegalEntityNo": row["LegalEntityNo"],
                        "Version type": row["Version type"],
                        "bp": values.index[i],
                        "prev Value": prev,
                        "act value": curr,
                        "next Value": next_,
                    }
                )

    return pd.DataFrame(anomalies)


# import excel file into pandas dictionary
excel_path_import = "./proalpha/SaaS Validation/2025-04-08 Controlling MRR output_EUR_v2.xlsx"
excel_path_anomalies = "./proalpha/SaaS Validation/Anomalies.xlsx"
sheets = pd.read_excel(excel_path_import,sheet_name=None)
logging.info(f"File {excel_path_import} imported. {len(sheets)} sheets found.")

# ignore first 2 sheets
sheet_names = list(sheets.keys())

# detect anomalies
anomalies = {}
for sheet_name in sheet_names:
    logging.info(f"Detect anomalies for sheet {sheet_name}")
    df = sheets[sheet_name]

    # Detect anomalies
    anomalies[sheet_name] = detect_anomalies(df)

with pd.ExcelWriter(excel_path_anomalies, engine="xlsxwriter") as writer:
    for sheet_name, df in anomalies.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)  
    
logging.info(f"File {excel_path_anomalies} exported")    

