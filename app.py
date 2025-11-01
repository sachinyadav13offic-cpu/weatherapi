from flask import Flask, render_template, request, jsonify


from datetime import datetime, timedelta
import pyodbc


app = Flask(__name__)


app.secret_key = "secret"

# ------------------ Configuration ------------------ #
VC_API_KEYS = [
    '6NSHBH2VCR2BPN6WMYRGLL4JX',
    '77EEYBH9HP5EFD3QJ44M7DX39',
    'E9VD8EY5W25NQJ4ATLYE4NB42',
    '7BSCDLVXUSBKW49LU5LNCJQYV',
    'JFSSVCVP2PZV6FL2HRPLMADAT',
    'DSR5VPHEX7JAARV7BET5TST9T',
    '9QE7RYKNFAR488VSFEP7MRZDG',
    'P8VTN4UHMDAMQHTFF9XMLQNMC',
    '2ECCLNJ89FMCU53G6VWLQCU5Q'
]

DB_CONFIG = {
    "server": "172.18.25.38",
    "user": "sa",
    "password": "wwilscada@4444",
    "database": "Weatherforecast"
}

def get_db_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};PWD={DB_CONFIG['password']};Trusted_Connection=no;"
    )
    return pyodbc.connect(conn_str)

# ------------------ Utility Functions ------------------ #
def convert_wind_direction(degrees):
    try:
        directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        idx = round(float(degrees) / 45) % 8
        return directions[idx]
    except Exception:
        return 'Unknown'

def fetch_weather_data(lat, lon):
    today = datetime.now().date()
    end_date = today + timedelta(days=4)
    for key in VC_API_KEYS:
        url = (
            f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
            f"{lat},{lon}/{today}/{end_date}?unitGroup=metric&key={key}"
        )
        try:
            response = requests.get(url, timeout=10) # type: ignore
            response.raise_for_status()
            print(f"✅ API key succeeded: {key}")
            return response.json()
        except Exception as e:
            print(f"⚠️ API key failed: {key} - {e}")
    raise Exception("❌ All API keys failed.")

def get_weather_icon(condition):
    condition = condition.lower()
    if 'sunny' in condition:
        return '01d'
    elif 'partly' in condition or 'cloudy' in condition:
        return '02d'
    elif 'rain' in condition:
        return '09d'
    elif 'storm' in condition or 'thunder' in condition:
        return '11d'
    elif 'snow' in condition:
        return '13d'
    elif 'fog' in condition or 'mist' in condition:
        return '50d'
    else:
        return '03d'

# ------------------ Routes ------------------ #
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/map")
def map_view():
    return render_template("map.html")

@app.route("/analysis")
def analysis_view():
    return render_template("analysis.html")

@app.route("/dashboard")
def dashboard():
    return render_template("graph.html")

@app.route("/view_data")
def view_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM WeatherData2 ORDER BY Createdon DESC")
    data = [dict(zip([column[0] for column in cursor.description], row)) for row in cursor.fetchall()]
    conn.close()
    return render_template("view_data.html", weather_data=data, get_weather_icon=get_weather_icon)

@app.route("/get_filter_hierarchy")
def get_filter_hierarchy():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT State, LOCNO, PlantNo
            FROM WeatherData2
            WHERE State IS NOT NULL AND LOCNO IS NOT NULL AND PlantNo IS NOT NULL
        """)
        rows = cursor.fetchall()
        hierarchy = {}
        for state, loc, plant in rows:
            hierarchy.setdefault(state, {}).setdefault(loc, []).append(plant)
        return jsonify(hierarchy)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

@app.route("/get_weather_by_location", methods=['GET'])
def get_weather_by_location():
    locno = request.args.get('locno')
    plantno = request.args.get('plantno')

    if not locno or not plantno:
        return jsonify({'error': 'Missing locno or plantno parameter'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.Weather_data @locno=?, @plantno=?", (locno, plantno))

        columns = [column[0].lower() for column in cursor.description]
        rows = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            rows.append({
                "ForecastDate": row_dict.get("forecastdate") or row_dict.get("forecast_date"),
                "Temp": row_dict.get("temp"),
                "TempMin": row_dict.get("tempmin") or row_dict.get("temp_min"),
                "TempMax": row_dict.get("tempmax") or row_dict.get("temp_max"),
                "Conditions": row_dict.get("conditions")
            })

        return jsonify(rows)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

@app.route("/search_area", methods=["GET"])
def search_area():
    import flask
    query = flask.request.args.get("q", "")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT  State, Area, Latitude, Longitude
            FROM WEC_All_Data
            WHERE State LIKE ? OR Area LIKE ?
        """, (f"%{query}%", f"%{query}%"))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        results = [
            {
                "state": row.State,
                "area": row.Area,
                "latitude": float(row.Latitude),
                "longitude": float(row.Longitude)
            }
            for row in rows
        ]
        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# @app.route("/get_map_locations", methods=["GET"])
# def get_map_locations():
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     cursor.execute("""
#         SELECT State, PlantNo, LOCNO, Latitude, Longitude
#         FROM WEC_All_Data_2
#         WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
#     """)
#     data = []
#     for row in cursor.fetchall():
#         data.append({
#             "state": row.State,
#             "plantno": row.PlantNo,
#             "locno": row.LOCNO,
#             "latitude": float(row.Latitude),
#             "longitude": float(row.Longitude)
#         })
#     conn.close()
#     return jsonify(data)


# 🔹 Convert DMS format to Decimal
def dms_to_decimal(dms_str):
    """
    Convert DMS like 'N22 4 55.4' or 'E75 46 32.1' to decimal degrees
    """
    try:
        parts = dms_str.strip().replace("°", "").split()
        
        # Hemisphere (N/S/E/W)
        hemisphere = parts[0][0].upper()
        
        # Extract degrees (remove hemisphere, e.g. 'N22' -> '22')
        degrees = float(parts[0][1:])
        minutes = float(parts[1]) if len(parts) > 1 else 0
        seconds = float(parts[2]) if len(parts) > 2 else 0

        decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)

        # South & West are negative
        if hemisphere in ['S', 'W']:
            decimal *= -1

        return decimal
    except Exception:
        return None

# 🔹 Safe conversion wrapper
def safe_latlon(value):
    try:
        return float(value)   # already decimal
    except ValueError:
        return dms_to_decimal(value)

# 🔹 Your API route
@app.route("/get_map_locations", methods=["GET"])
def get_map_locations():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT State, PlantNo, LOCNO, Latitude, Longitude
        FROM WEC_All_Data_2
        WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
    """)
    data = []
    for row in cursor.fetchall():
        lat = safe_latlon(row.Latitude)
        lon = safe_latlon(row.Longitude)
        if lat is not None and lon is not None:
            data.append({
                "state": row.State,
                "plantno": row.PlantNo,
                "locno": row.LOCNO,
                "latitude": lat,
                "longitude": lon
            })
    conn.close()
    return jsonify(data)



# ------------------ Scheduled Job ------------------ #
def save_weather_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        print(f"⏳ Running weather update at {datetime.now()}")
        cursor.execute("""
            SELECT DISTINCT State, LOCNO, PlantNo, Latitude, Longitude
            FROM WEC_All_Data_2
            WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
        """)
        records = cursor.fetchall()
        count = 0

        for state, locno, plantno, lat, lon in records:
            try:
                data = fetch_weather_data(lat, lon)
                for day in data.get("days", []):
                    forecast_date = datetime.strptime(day["datetime"], "%Y-%m-%d")
                    cursor.execute("""
                        INSERT INTO WeatherData2 (
                            State, LOCNO, PlantNo, Latitude, Longitude, WindSpeed, WindGust,
                            WindDir, Conditions, Temp, TempMin, TempMax, Humidity, Precip,
                            Createdon, ForecastDate
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        state, locno, plantno, lat, lon,
                        day.get("windspeed", 0.0),
                        day.get("windgust", 0.0),
                        convert_wind_direction(day.get("winddir", 0)),
                        day.get("conditions", "Unknown"),
                        day.get("temp", 0.0),
                        day.get("tempmin", 0.0),
                        day.get("tempmax", 0.0),
                        day.get("humidity", 0.0),
                        day.get("precip", 0.0),
                        datetime.now(),
                        forecast_date
                    ))
                    count += 1
            except Exception as e:
                print(f"[Error] Skipped {state}-{locno}: {e}")
        conn.commit()
        print(f"✅ Inserted {count} weather records.")
    except Exception as e:
        print(f"[Error] save_weather_data failed: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

# ------------------ Run the App ------------------ #
if __name__ == "__main__":
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(save_weather_data, 'cron', hour=10, minute=10)
    # scheduler.start()
    # NOTE: ssl_context='adhoc' enables a temporary self-signed cert for HTTPS (development only).
    # Browsers will show a certificate warning which you must accept to use geolocation over HTTPS.
        import os
        use_ssl = str(os.environ.get("USE_SSL", "0")).lower() in ("1", "true", "yes")
        host = os.environ.get("HOST", "0.0.0.0")
        port = int(os.environ.get("PORT", "6013"))
        if use_ssl:
            # Only enable adhoc for development debugging when explicitly requested via USE_SSL=1
            print("Starting Flask with adhoc SSL (development only)")
            app.run(debug=True, host=host, port=port, ssl_context='adhoc')
        else:
            print(f"Starting Flask on http://{host}:{port} (USE_SSL not set)")
            app.run(debug=True, host=host, port=port)
