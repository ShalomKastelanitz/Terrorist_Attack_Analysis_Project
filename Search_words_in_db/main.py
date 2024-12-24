from flask import Flask, request, jsonify
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, or_, and_
from Saving_data_from_API.db_utils import session, FactEvents, DimLocation, DimDate
import datetime
from db.utils import engine
app = Flask(__name__)

Session = sessionmaker(bind=engine)
session = Session()

def format_event_result(event):
    return {
        "event_id": event.event_id,
        "date": f"{event.date.iyear}-{event.date.imonth}-{event.date.iday}",
        "location": {
            "city": event.location.city,
            "country": event.location.country_txt,
            "latitude": event.location.latitude,
            "longitude": event.location.longitude,
        },
        "attack_type": event.attack_type.attacktype_name if event.attack_type else "Unknown",
        "group": event.primary_group.group_name if event.primary_group else "Unknown",
        "fatalities": event.nkill,
        "injuries": event.nwound,
        "description": event.description,
    }

# פונקציה לחיפוש לפי מילות מפתח
def search_events_by_keywords(keywords, limit=None, start_date=None, end_date=None):
    query = session.query(FactEvents).join(DimLocation).join(DimDate)

    if keywords:
        query = query.filter(
            or_(
                FactEvents.description.ilike(f"%{keywords}%"),
                DimLocation.city.ilike(f"%{keywords}%"),
                DimLocation.country_txt.ilike(f"%{keywords}%"),
            )
        )

    if start_date and end_date:
        query = query.filter(
            and_(
                DimDate.iyear >= start_date.year,
                DimDate.iyear <= end_date.year,
                DimDate.imonth >= start_date.month,
                DimDate.imonth <= end_date.month,
                DimDate.iday >= start_date.day,
                DimDate.iday <= end_date.day,
            )
        )

    if limit:
        query = query.limit(limit)

    return [format_event_result(event) for event in query.all()]

@app.route('/search/keywords', methods=['GET'])
def search_keywords():
    keywords = request.args.get('keywords', '')
    limit = request.args.get('limit', type=int)
    results = search_events_by_keywords(keywords, limit=limit)
    return jsonify(results)

@app.route('/search/news', methods=['GET'])
def search_news():
    keywords = request.args.get('keywords', '')
    limit = request.args.get('limit', type=int)
    return jsonify({"message": "Search news not yet implemented"})

@app.route('/search/historic', methods=['GET'])
def search_historic():
    keywords = request.args.get('keywords', '')
    limit = request.args.get('limit', type=int)
    results = search_events_by_keywords(keywords, limit=limit)
    return jsonify(results)

@app.route('/search/combined', methods=['GET'])
def search_combined():
    keywords = request.args.get('keywords', '')
    limit = request.args.get('limit', type=int)
    start_date = request.args.get('start_date', type=str)
    end_date = request.args.get('end_date', type=str)

    if start_date:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    if end_date:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    results = search_events_by_keywords(keywords, limit=limit, start_date=start_date, end_date=end_date)
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
