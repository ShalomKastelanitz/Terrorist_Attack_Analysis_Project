from sqlalchemy.orm import sessionmaker
from db.schema.create_tables import Base, DimDate, DimLocation, DimGroup, DimAttackType, FactEvents

from db.utils import engine
Session = sessionmaker(bind=engine)
session = Session()

def get_or_create_date(date_str):
    from datetime import datetime
    date = datetime.strptime(date_str, "%Y-%m-%d")
    dim_date = session.query(DimDate).filter_by(iyear=date.year, imonth=date.month, iday=date.day).first()
    if not dim_date:
        new_date_id = session.query(DimDate).count() + 1
        dim_date = DimDate(date_id=new_date_id, iyear=date.year, imonth=date.month, iday=date.day)
        session.add(dim_date)
        session.flush()
        print(f"[INFO] Added new date with ID {dim_date.date_id}: {dim_date}")
    return dim_date.date_id

def get_or_create_location(region, country, city, latitude, longitude):
    location = session.query(DimLocation).filter_by(region_txt=region, country_txt=country, city=city).first()
    if not location:
        new_location_id = session.query(DimLocation).count() + 1
        location = DimLocation(location_id=new_location_id, region_txt=region, country_txt=country, city=city, latitude=latitude, longitude=longitude)
        session.add(location)
        session.flush()
        print(f"[INFO] Added new location with ID {location.location_id}: {location}")
    return location.location_id

def get_or_create_group(group_name):
    group = session.query(DimGroup).filter_by(group_name=group_name).first()
    if not group:
        new_group_id = session.query(DimGroup).count() + 1
        group = DimGroup(group_id=new_group_id, group_name=group_name)
        session.add(group)
        session.flush()
        print(f"[INFO] Added new group with ID {group.group_id}: {group}")
    return group.group_id

def get_or_create_attack_type(attack_type_name):
    attack_type = session.query(DimAttackType).filter_by(attacktype_name=attack_type_name).first()
    if not attack_type:
        new_attack_type_id = session.query(DimAttackType).count() + 1
        attack_type = DimAttackType(attacktype_id=new_attack_type_id, attacktype_name=attack_type_name)
        session.add(attack_type)
        session.flush()
        print(f"[INFO] Added new attack type with ID {attack_type.attacktype_id}: {attack_type}")
    return attack_type.attacktype_id

def insert_fact_event(date_id, location_id, attack_type_id, group_id, nkill, nwound, article_id):
    existing_event = session.query(FactEvents).filter_by(event_id=article_id).first()
    if not existing_event:
        event = FactEvents(
            event_id=article_id,
            date_id=date_id,
            location_id=location_id,
            attacktype_id=attack_type_id,
            primary_group_id=group_id,
            nkill=nkill,
            nwound=nwound
        )
        session.add(event)
        session.flush()
        print(f"[INFO] Added new event with ID {event.event_id}: {event}")
