import pandas as pd
from pathlib import Path

raw_path = Path("../data(csv)/globalterrorismdb_0718dist.csv")
intermediate_path = Path("data/intermediate/cleaned_terror_data.csv")
processed_path = Path("data/processed/")

# עמודות
columns_needed = [
    "eventid",
    "iyear", "imonth", "iday",
    "region_txt", "country_txt", "city",
    "latitude", "longitude",
    "attacktype1_txt", "targtype1_txt",
    "gname", "gname2", "gname3",
    "nkill", "nwound"
]


def clean_and_filter(df):
    df = df[columns_needed].copy()

    # הסרת רשומות ללא eventid
    #df = df.dropna(subset=["eventid"])

    numeric_cols = ["iyear", "imonth", "iday", "nkill", "nwound", "latitude", "longitude"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df["nkill"] = df["nkill"].fillna(0)
    df["nwound"] = df["nwound"].fillna(0)

    # טיפול בערכי טקסט ריקים
    df["city"] = df["city"].fillna("Unknown").str.strip()
    df["gname"] = df["gname"].fillna("Unknown").str.strip()
    df["gname2"] = df["gname2"].fillna("None").str.strip()
    df["gname3"] = df["gname3"].fillna("None").str.strip()
    df["region_txt"] = df["region_txt"].fillna("Unknown").str.strip()
    df["country_txt"] = df["country_txt"].fillna("Unknown").str.strip()
    df["attacktype1_txt"] = df["attacktype1_txt"].fillna("Unknown").str.strip()
    df["targtype1_txt"] = df["targtype1_txt"].fillna("Unknown").str.strip()

    df = df.dropna(subset=["latitude", "longitude"])

    # פילטר: latitude בין -90 ל-90 ו-longitude בין -180 ל-180
    df = df[(df["latitude"].between(-90, 90)) & (df["longitude"].between(-180, 180))]

    return df


def create_dimensions_and_fact(df):
    # יצירת dim_date
    dim_date = df[["iyear", "imonth", "iday"]].drop_duplicates().copy()
    dim_date["date_id"] = range(1, len(dim_date) + 1)

    # יצירת dim_location
    dim_location = df[["region_txt", "country_txt", "city", "latitude", "longitude"]].drop_duplicates().copy()
    dim_location["location_id"] = range(1, len(dim_location) + 1)

    # dim_attack_type
    dim_attack_type = df[["attacktype1_txt"]].drop_duplicates().copy()
    dim_attack_type["attacktype_id"] = range(1, len(dim_attack_type) + 1)
    dim_attack_type = dim_attack_type.rename(columns={"attacktype1_txt": "attacktype_name"})

    # dim_target_type
    dim_target_type = df[["targtype1_txt"]].drop_duplicates().copy()
    dim_target_type["targettype_id"] = range(1, len(dim_target_type) + 1)
    dim_target_type = dim_target_type.rename(columns={"targtype1_txt": "targettype_name"})

    # dim_group
    groups = pd.concat([df["gname"], df["gname2"], df["gname3"]]).drop_duplicates()
    # הסרת "None" אם רוצים
    groups = groups[groups != "None"]
    dim_group = pd.DataFrame(groups, columns=["group_name"]).drop_duplicates()
    dim_group["group_id"] = range(1, len(dim_group) + 1)

    # כעת ניצור fact_events
    # נדרשים למפות כל רשומה ל-ID של כל מימד
    # נעשה Merge כדי להחליף טקסט ב-ID

    # Merge dim_date
    fact = df.merge(dim_date, how="left", on=["iyear", "imonth", "iday"])

    # Merge dim_location
    fact = fact.merge(dim_location, how="left", on=["region_txt", "country_txt", "city", "latitude", "longitude"])

    # Merge dim_attack_type
    fact = fact.merge(dim_attack_type, how="left", left_on="attacktype1_txt", right_on="attacktype_name")

    # Merge dim_target_type
    fact = fact.merge(dim_target_type, how="left", left_on="targtype1_txt", right_on="targettype_name")


    group_dict = dim_group.set_index("group_name")["group_id"].to_dict()

    fact["primary_group_id"] = fact["gname"].map(group_dict)
    fact["secondary_group_id"] = fact["gname2"].map(group_dict)
    fact["tertiary_group_id"] = fact["gname3"].map(group_dict)

    fact_events = fact[[
        "eventid",
        "date_id",
        "location_id",
        "attacktype_id",
        "targettype_id",
        "primary_group_id",
        "secondary_group_id",
        "tertiary_group_id",
        "nkill",
        "nwound"
    ]].drop_duplicates()

    fact_events = fact_events.rename(columns={"eventid": "event_id"})

    return dim_date, dim_location, dim_attack_type, dim_target_type, dim_group, fact_events


if __name__ == "__main__":
    df = pd.read_csv(raw_path, encoding='latin1', low_memory=False)
    df = clean_and_filter(df)

    intermediate_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(intermediate_path, index=False)

    processed_path.mkdir(parents=True, exist_ok=True)

    dim_date, dim_location, dim_attack_type, dim_target_type, dim_group, fact_events = create_dimensions_and_fact(df)

    dim_date.to_csv(processed_path / "dim_date.csv", index=False)
    dim_location.to_csv(processed_path / "dim_location.csv", index=False)
    dim_attack_type.to_csv(processed_path / "dim_attack_type.csv", index=False)
    dim_target_type.to_csv(processed_path / "dim_target_type.csv", index=False)
    dim_group.to_csv(processed_path / "dim_group.csv", index=False)
    fact_events.to_csv(processed_path / "fact_events.csv", index=False)

    print("Transform completed successfully!")
