from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc
from collections import defaultdict
from sqlalchemy.exc import InvalidRequestError
from db.utils import get_session
from db.schema.create_tables import FactEvents, DimDate, DimLocation, DimAttackType, DimTargetType, DimGroup
import pandas as pd

group_B_Blueprint = Blueprint('group_B', __name__)

# 11. זיהוי קבוצות עם מטרות משותפות באותו אזור
import pandas as pd
from sqlalchemy import func


def groups_with_same_targets_in_region(region=None, city=None):
    session = get_session()
    q = (
        session.query(
            DimLocation.region_txt,
            DimTargetType.targettype_name,
            DimGroup.group_name,
            func.avg(DimLocation.latitude).label('avg_lat'),
            func.avg(DimLocation.longitude).label('avg_lon')
        )
        .select_from(FactEvents)
        .join(DimLocation, FactEvents.location_id == DimLocation.location_id)
        .join(DimTargetType, FactEvents.targettype_id == DimTargetType.targettype_id)
        .join(DimGroup, FactEvents.primary_group_id == DimGroup.group_id)
        .group_by(
            DimLocation.region_txt,
            DimTargetType.targettype_name,
            DimGroup.group_name
        )
    )

    if region:
        q = q.filter(DimLocation.region_txt.ilike(f"%{region}%"))

    if city:
        q = q.filter(DimLocation.city.ilike(f"%{city}%"))

    rows = q.all()
    session.close()

    return rows


# 13. איתור קבוצות שהשתתפו באותה תקיפה
@group_B_Blueprint.route('/groups_in_same_attack/<int:event_id>', methods=['GET'])
def groups_in_same_attack(event_id):
    session = get_session()
    try:
        event = (
            session.query(
                FactEvents.primary_group_id,
                FactEvents.secondary_group_id,
                FactEvents.tertiary_group_id
            )
            .filter(FactEvents.event_id == event_id)
            .first()
        )

        if not event:
            return jsonify({"error": "Event not found"}), 404

        # הוצאת הקבוצות שאינן None
        group_ids = [group_id for group_id in event if group_id is not None]

        # שאילתה לקבלת שמות הקבוצות
        groups = (
            session.query(DimGroup.group_name)
            .filter(DimGroup.group_id.in_(group_ids))
            .all()
        )

        session.close()

        # החזרת התוצאה בפורמט JSON
        return jsonify({
            "event_id": event_id,
            "groups_involved": [group[0] for group in groups]
        })

    except Exception as e:
        session.close()
        print(f"Error in groups_in_same_attack: {e}")
        return jsonify({"error": "An error occurred"}), 500


# 14. זיהוי אזורים עם אסטרטגיות תקיפה משותפות
def regions_with_most_unique_groups(region=None, city=None, limit=20):
    session = get_session()

    # שאילתה למציאת כמות הקבוצות הייחודיות לפי אזור וסוג התקפה
    q = (
        session.query(
            DimLocation.region_txt,
            DimAttackType.attacktype_name,
            func.count(DimGroup.group_id.distinct()).label("unique_group_count"),
            func.array_agg(DimGroup.group_name.distinct()).label("group_names"),
            func.avg(DimLocation.latitude).label("avg_lat"),
            func.avg(DimLocation.longitude).label("avg_lon"),
        )
        .select_from(FactEvents)
        .join(DimLocation, FactEvents.location_id == DimLocation.location_id)
        .join(DimAttackType, FactEvents.attacktype_id == DimAttackType.attacktype_id)
        .join(DimGroup, FactEvents.primary_group_id == DimGroup.group_id)
        .group_by(
            DimLocation.region_txt,
            DimAttackType.attacktype_name
        )
        .order_by(func.count(DimGroup.group_id.distinct()).desc())  # סידור לפי כמות הקבוצות הייחודיות
        .limit(limit)
    )

    # סינון לפי region אם סופק
    if region:
        q = q.filter(DimLocation.region_txt.ilike(f"%{region}%"))

    # סינון לפי city אם סופק
    if city:
        q = q.filter(DimLocation.city.ilike(f"%{city}%"))

    rows = q.all()
    session.close()

    return rows


# 15. איתור קבוצות עם העדפות מטרות דומות
@group_B_Blueprint.route('/groups_with_similar_target_preferences', methods=['GET'])
def groups_with_similar_target_preferences():
    session = get_session()
    q = (session.query(DimGroup.group_name, DimTargetType.targettype_name, func.count(FactEvents.event_id))
         .join(DimGroup, FactEvents.primary_group_id == DimGroup.group_id)
         .join(DimTargetType, FactEvents.targettype_id == DimTargetType.targettype_id)
         .group_by(DimGroup.group_name, DimTargetType.targettype_name))

    rows = q.all()
    data = []
    for g,t,cnt in rows:
        data.append({"group_name": g, "target_type": t, "count_events": cnt})
    session.close()
    return jsonify(data)

# 16. זיהוי אזורים עם פעילות בין-קבוצתית גבוהה
def regions_with_high_group_diversity(region=None, country=None, city=None, limit=20):
    session = get_session()

    q = (
        session.query(
            DimLocation.region_txt,
            DimLocation.country_txt,
            func.count(DimGroup.group_id.distinct()).label("unique_group_count"),
            func.array_agg(DimGroup.group_name.distinct()).label("group_names"),
            func.avg(DimLocation.latitude).label("avg_lat"),
            func.avg(DimLocation.longitude).label("avg_lon")
        )
        .select_from(FactEvents)
        .join(DimLocation, FactEvents.location_id == DimLocation.location_id)
        .join(DimGroup, FactEvents.primary_group_id == DimGroup.group_id)
        .group_by(
            DimLocation.region_txt,
            DimLocation.country_txt
        )
        .order_by(func.count(DimGroup.group_id.distinct()).desc())  # סדר לפי כמות הקבוצות הייחודיות
    )

    # סינון לפי region אם נדרש
    if region:
        q = q.filter(DimLocation.region_txt.ilike(f"%{region}%"))
    # סינון לפי country אם נדרש
    if country:
        q = q.filter(DimLocation.country_txt.ilike(f"%{country}%"))
    # סינון לפי city אם נדרש
    if city:
        q = q.filter(DimLocation.city.ilike(f"%{city}%"))

    # הגבלת מספר תוצאות
    if limit:
        q = q.limit(limit)

    rows = q.all()
    session.close()

    return rows
