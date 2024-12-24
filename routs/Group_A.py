from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc
from collections import defaultdict
from db.utils import get_session
from db.schema.create_tables import FactEvents, DimDate, DimLocation, DimAttackType, DimTargetType, DimGroup

group_A_Blueprint = Blueprint('group_A', __name__)


# סוגי ההתקפה הקטלניים ביותר1
@group_A_Blueprint.route('/most_lethal_attack_types', methods=['GET'])
def most_lethal_attack_types():
    session = get_session()
    q = (session.query(DimAttackType.attacktype_name,
                       (func.sum(FactEvents.nkill)*2 + func.sum(FactEvents.nwound)).label('severity'))
         .join(DimAttackType, FactEvents.attacktype_id == DimAttackType.attacktype_id)
         .group_by(DimAttackType.attacktype_name)
         .order_by(desc('severity')))
    top = q.all()
    result = [{"attacktype": row[0], "severity_score": int(row[1])} for row in top]
    session.close()
    return jsonify(result)

# 2 ממוצע נפגעים לפי אזור (2*nkill + nwound)
#@group_A_Blueprint.route('/avg_casualties_per_region', methods=['GET'])
def avg_casualties_per_region(region=None, top=None):
    session = get_session()
    severity_expr = (2 * FactEvents.nkill + FactEvents.nwound)
    q = (
        session.query(
            DimLocation.country_txt,
            func.avg(severity_expr).label('avg_severity'),
            func.avg(DimLocation.latitude).label('avg_lat'),
            func.avg(DimLocation.longitude).label('avg_lon')
        )
        .join(DimLocation, FactEvents.location_id == DimLocation.location_id)
        .group_by(DimLocation.country_txt)
        .order_by(desc('avg_severity'))
    )
    if region:
        q = q.filter(DimLocation.region_txt.ilike(f"%{region}%"))
    rows = q.all()
    session.close()

    if top:
        rows = rows[:top]

    return rows


# 3. חמש הקבוצות עם הכי הרבה נפגעים
@group_A_Blueprint.route('/top_5_groups_casualties', methods=['GET'])
def top_5_groups_casualties():
    session = get_session()
    q = (session.query(DimGroup.group_name, func.sum(FactEvents.nkill+FactEvents.nwound).label('total_casualties'))
         .join(DimGroup, FactEvents.primary_group_id == DimGroup.group_id)
         .group_by(DimGroup.group_name)
         .order_by(desc('total_casualties'))
         .limit(5))
    result = [{"group_name": row[0], "total_casualties": int(row[1])} for row in q]
    session.close()
    return jsonify(result)

# 5. מגמות שנתיות וחודשיות בתדירות התקפות
@group_A_Blueprint.route('/attack_trends', methods=['GET'])
def attack_trends():
    session = get_session()
    q = (session.query(DimDate.iyear, DimDate.imonth, func.count(FactEvents.event_id).label('num_events'))
         .join(DimDate, FactEvents.date_id == DimDate.date_id)
         .group_by(DimDate.iyear, DimDate.imonth)
         .order_by(DimDate.iyear, DimDate.imonth))
    data = [{"year": row[0], "month": row[1], "num_events": row[2]} for row in q]
    session.close()
    return jsonify(data)

# 6. אחוז שינוי במספר הפיגועים בין שנים לפי אזור
def yearly_change_per_region(region=None, top=None):
    """
    מחזיר רשימת tuples בפורמט:
    [
      (region_name, avg_change_percent, lat, lon),
      ...
    ]
    כאשר avg_change_percent הוא הממוצע של אחוזי השינוי בין כל זוג שנים עוקבות.
    """
    session = get_session()
    # שולפים: region, year, count(event_id), avg(lat), avg(lon)
    q = (
        session.query(
            DimLocation.region_txt,
            DimDate.iyear,
            func.count(FactEvents.event_id).label("count_events"),
            func.avg(DimLocation.latitude).label("avg_lat"),
            func.avg(DimLocation.longitude).label("avg_lon")
        )
        .join(DimDate, FactEvents.date_id == DimDate.date_id)
        .join(DimLocation, FactEvents.location_id == DimLocation.location_id)
        .group_by(DimLocation.region_txt, DimDate.iyear)
    )

    if region:
        q = q.filter(DimLocation.region_txt.ilike(f"%{region}%"))

    raw_rows = q.all()
    session.close()

    # raw_rows -> List[ (region_txt, year, count_events, avg_lat, avg_lon), ... ]
    # בניית מבנה לאגירת הנתונים לפי region:
    data_dict = {}
    for (r, y, cnt, lat, lon) in raw_rows:
        if r not in data_dict:
            data_dict[r] = {
                "coords": (lat, lon),
                "counts_by_year": {}
            }
        data_dict[r]["counts_by_year"][y] = cnt

    final_rows = []

    for r, vals in data_dict.items():
        years_sorted = sorted(vals["counts_by_year"].keys())
        lat, lon = vals["coords"]
        changes = []

        # חישוב שינוי יחסי לכל זוג שנים עוקבות
        for i in range(1, len(years_sorted)):
            prev_year = years_sorted[i-1]
            curr_year = years_sorted[i]
            prev_count = vals["counts_by_year"][prev_year]
            curr_count = vals["counts_by_year"][curr_year]
            if prev_count != 0:
                change_percent = ((curr_count - prev_count) / prev_count) * 100
                changes.append(change_percent)

        # ממוצע השינויים (אם יש בכלל יותר משנה אחת)
        avg_change = sum(changes)/len(changes) if changes else 0.0
        final_rows.append((r, avg_change, lat, lon))

    # סינון Top אם צריך
    if top:
        # נמיין לפי ערך השינוי בסדר יורד
        final_rows.sort(key=lambda x: abs(x[1]), reverse=True)
        final_rows = final_rows[:top]

    return final_rows

def most_active_groups_by_region(region=None, limit=5):
    session = get_session()

    q = (
        session.query(
            DimLocation.region_txt,
            DimGroup.group_name,
            func.count(FactEvents.event_id).label("event_count"),
            func.avg(DimLocation.latitude).label("avg_lat"),
            func.avg(DimLocation.longitude).label("avg_lon")
        )
        .select_from(FactEvents)
        .join(DimLocation, FactEvents.location_id == DimLocation.location_id)
        .join(DimGroup, FactEvents.primary_group_id == DimGroup.group_id)
        .group_by(DimLocation.region_txt, DimGroup.group_name)
        .order_by(func.count(FactEvents.event_id).desc())
    )

    if region:
        q = q.filter(DimLocation.region_txt.ilike(f"%{region}%"))

    rows = q.all()
    session.close()

    # קיבוץ נתונים לפי אזור ושמירה של Top-N קבוצות
    region_grouped_data = {}
    for r, group_name, event_count, lat, lon in rows:
        if r not in region_grouped_data:
            region_grouped_data[r] = []
        region_grouped_data[r].append((group_name, event_count, lat, lon))

    # החזרת חמשת הקבוצות המובילות לכל אזור
    for r in region_grouped_data:
        region_grouped_data[r] = region_grouped_data[r][:limit]

    return region_grouped_data
