import pandas as pd
from pathlib import Path

def extract_raw_data():
    raw_file = Path("data/raw/globalterrorismdb_1970_2022.csv")
    df = pd.read_csv(raw_file, encoding='latin1', low_memory=False)
    return df

if __name__ == "__main__":
    df = extract_raw_data()
    print("Number of rows in raw data:", len(df))
