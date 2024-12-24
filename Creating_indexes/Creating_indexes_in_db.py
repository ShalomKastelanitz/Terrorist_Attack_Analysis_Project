from sqlalchemy import create_engine, text
from db.utils import engine


index_queries = [
    "CREATE INDEX IF NOT EXISTS idx_factevents_location_id ON fact_events(location_id);",
    "CREATE INDEX IF NOT EXISTS idx_factevents_targettype_id ON fact_events(targettype_id);",
    "CREATE INDEX IF NOT EXISTS idx_factevents_primary_group_id ON fact_events(attacktype_id);",

    # אינדקס בטבלת DimLocation
    "CREATE INDEX IF NOT EXISTS idx_dimlocation_region_txt ON dim_location(region_txt);",
]

with engine.connect() as connection:
    for query in index_queries:
        try:
            connection.execute(text(query))
            print(f"Executed: {query}")
        except Exception as e:
            print(f"Error executing query: {query}\n{e}")

engine.dispose()

print("Index creation completed.")
