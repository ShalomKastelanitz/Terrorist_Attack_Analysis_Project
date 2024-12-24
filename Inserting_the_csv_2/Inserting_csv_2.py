import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from Saving_data_from_API.db_utils import (
    session,
    get_or_create_date,
    get_or_create_location,
    get_or_create_group,
    get_or_create_attack_type,
    insert_fact_event
)

# חיבור לדאטהבייס
engine = create_engine("postgresql://postgres:1234@localhost/wwii_missions3")
Session = sessionmaker(bind=engine)
session = Session()


def load_additional_data(file_path):
    try:
        data = pd.read_csv(file_path, encoding='latin1')  # נסה עם קידוד מתאים
        print(f"[INFO] Loaded {len(data)} records from {file_path}")
        return data
    except Exception as e:
        print(f"[ERROR] Failed to load data from {file_path}: {e}")
        raise

# עיבוד הנתונים החדשים

def process_additional_data(file_path):
    data = load_additional_data(file_path)

    for index, row in data.iterrows():
        try:
            date_str = row['Date']
            date = pd.to_datetime(date_str, format='%d-%b-%y', errors='coerce')
            if pd.isnull(date):
                raise ValueError(f"Invalid date format: {date_str}")
            date_id = get_or_create_date(date.strftime('%Y-%m-%d'))

            city = row['City']
            country = row['Country']
            location_id = get_or_create_location(
                region="Unknown",
                country=country,
                city=city,
                latitude=None,
                longitude=None
            )

            # סוג התקפה ונשק
            attack_type_id = get_or_create_attack_type(row.get('Weapon', 'Unknown'))
            group_id = get_or_create_group(row.get('Perpetrator', 'Unknown'))

            # פרטי האירוע
            injuries = int(row.get('Injuries', 0)) if pd.notnull(row.get('Injuries')) else 0
            fatalities = int(row.get('Fatalities', 0)) if pd.notnull(row.get('Fatalities')) else 0
            description = row.get('Description', '')

            # הוספת האירוע
            max_event_id_query = session.execute("SELECT MAX(event_id) FROM fact_events").scalar()
            event_id = max_event_id_query + 1 if max_event_id_query else 1
            insert_fact_event(
                date_id=date_id,
                location_id=location_id,
                attack_type_id=attack_type_id,
                group_id=group_id,
                nkill=fatalities,
                nwound=injuries,
                article_id=event_id
            )

            session.commit()
            print(f"[INFO] Processed record {index + 1}/{len(data)}")

        except Exception as e:
            session.rollback()
            print(f"[ERROR] Failed to process record {index + 1}: {e}")

    print(f"[INFO] Successfully processed all records.")

def main():

    file_path = "../data(csv)/RAND_Database_of_Worldwide_Terrorism_Incidents - 5000 rows (1).csv"  # עדכן עם המיקום האמיתי של הקובץ
    process_additional_data(file_path)

if __name__ == "__main__":
    main()
