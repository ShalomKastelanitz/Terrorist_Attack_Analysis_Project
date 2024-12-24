import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path


processed_path = Path("data/processed/")

engine = create_engine("postgresql://postgres:1234@localhost/wwii_missions3")

def load_table(table_name, csv_file):
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"{table_name} loaded successfully with {len(df)} rows.")

if __name__ == "__main__":
    load_table("dim_date", processed_path / "dim_date.csv")
    load_table("dim_location", processed_path / "dim_location.csv")
    load_table("dim_attack_type", processed_path / "dim_attack_type.csv")
    load_table("dim_target_type", processed_path / "dim_target_type.csv")
    load_table("dim_group", processed_path / "dim_group.csv")
    load_table("fact_events", processed_path / "fact_events.csv")

    print("All tables loaded successfully!")
