from flask import Flask, render_template, request, url_for, jsonify
import folium
import os
from sqlalchemy import create_engine, func, desc
from sqlalchemy.orm import sessionmaker
from db.schema.create_tables import FactEvents, DimLocation, DimDate, DimGroup, DimAttackType, DimTargetType
from db.utils import get_session
from routs.Group_A import group_A_Blueprint, avg_casualties_per_region, yearly_change_per_region, most_active_groups_by_region
from routs.Group_B import group_B_Blueprint, groups_with_same_targets_in_region, regions_with_most_unique_groups, \
    regions_with_high_group_diversity

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def home():
    session = get_session()

    query_type = request.form.get("query", None)
    region = request.form.get("region", None)
    country=request.form.get("country", None)
    top = request.form.get("top", None, type=int)

    my_map = folium.Map(location=[20,0], zoom_start=2)

    if query_type == "avg_casualties_per_region":
        rows = avg_casualties_per_region(region=region, top=top)

        for r, avg_s, lat, lon in rows:
            if lat and lon:
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=max(5, min(20, avg_s/10)),
                    popup=f"Region: {r}, Avg Casualties: {avg_s:.2f}",
                    color="red",
                    fill=True,
                    fill_color="red"
                ).add_to(my_map)

    elif query_type == "yearly_change_per_region":
        rows = yearly_change_per_region(region=region, top=top)
        for r, avg_ch, lat, lon in rows:
            if lat and lon:
                color = "green" if avg_ch > 0 else "blue"
                desc = f"Region: {r}<br>Avg change: {avg_ch:.2f}%"
                folium.Marker(
                    location=[lat, lon],
                    popup=desc,
                    icon=folium.Icon(color=color)
                ).add_to(my_map)

    elif query_type == "most_active_groups_by_region":
        region = request.args.get('region', None)
        region_data = most_active_groups_by_region(region=region, limit=5)

        for region, groups in region_data.items():
            top_group = groups[0]
            lat, lon = top_group[2], top_group[3]
            popup_content = (
                    f"Region: {region}<br>"
                    f"Top Group: {top_group[0]} ({top_group[1]} events)<br>"
                    f"<b>Top 5 Groups:</b><br>"
                    + "<br>".join([f"{g[0]}: {g[1]} events" for g in groups])
            )

            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(popup_content, max_width=300),
                icon=folium.Icon(color="red")
            ).add_to(my_map)


    elif query_type == "groups_with_same_targets_in_region":

        region = request.args.get('region', None)

        city = request.args.get('city', None)

        rows = groups_with_same_targets_in_region(region=region, city=city)
        rows=rows[:20]

        for r, t, g, lat, lon in rows:

            if lat and lon:
                popup_content = f"Region: {r}<br>Target: {t}<br>Group: {g}"

                folium.Marker(

                    location=[lat, lon],

                    popup=popup_content,

                    icon=folium.Icon(color="blue")

                ).add_to(my_map)



    elif query_type == "regions_with_most_unique_groups":

        region = request.args.get('region', None)

        city = request.args.get('city', None)  # הוספת אפשרות לקבלת עיר מהפרמטרים

        rows = regions_with_most_unique_groups(region=region, city=city, limit=20)  # מגבלה ל-20 תוצאות

        for region, attack_type, unique_count, group_names, lat, lon in rows:

            if lat and lon:
                group_list = "<br>".join(group_names)

                popup_content = (

                    f"Region: {region}<br>"

                    f"Attack Type: {attack_type}<br>"

                    f"Unique Groups: {unique_count}<br>"

                    f"<b>Groups:</b><br>{group_list}"

                )

                # הוספת marker למפה

                folium.Marker(

                    location=[lat, lon],

                    popup=folium.Popup(popup_content, max_width=300),

                    icon=folium.Icon(color="purple")

                ).add_to(my_map)






    elif query_type == "regions_with_high_group_diversity":

        region = request.args.get('region', None)

        country = request.args.get('country', None)

        city = request.args.get('city', None)  # הוספת אפשרות לקבלת עיר מהפרמטרים

        rows = regions_with_high_group_diversity(region=region, country=country, city=city, limit=20)

        for r, c, unique_count, group_names, lat, lon in rows:

            if lat and lon:
                # רשימת הקבוצות להצגה בחלון הקופץ

                group_list = "<br>".join(group_names)

                popup_content = (

                    f"Region: {r}<br>"

                    f"Country: {c}<br>"

                    f"Unique Groups: {unique_count}<br>"

                    f"<b>Groups:</b><br>{group_list}"

                )

                # הוספת marker למפה

                folium.Marker(

                    location=[lat, lon],

                    popup=folium.Popup(popup_content, max_width=300),

                    icon=folium.Icon(color="orange")

                ).add_to(my_map)

    # שמירת המפה
    map_path = os.path.join("templates", "map.html")
    my_map.save(map_path)

    session.close()
    return render_template("index.html")
app.register_blueprint(group_A_Blueprint, url_prefix='/api')
app.register_blueprint(group_B_Blueprint, url_prefix='/api')

@app.route("/render_map")
def render_map():
    return render_template("map.html")

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
