from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
import logging
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            response = requests.get(url, timeout=10)
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

def safe_latlon(value):
    try:
        return float(value)   # already decimal
    except ValueError:
        return dms_to_decimal(value)

# === Dashboard Specific Functions ===
def get_actual_columns():
    """Get actual column names from the database"""
    try:
        conn = get_db_connection()
        query = "SELECT TOP 1 * FROM weatherdata2"
        df = pd.read_sql(query, conn)
        conn.close()
        
        actual_columns = {
            'all_columns': df.columns.tolist(),
            'sample_data': df.iloc[0].to_dict() if not df.empty else {}
        }
        
        logger.info(f"Actual columns: {actual_columns['all_columns']}")
        return actual_columns
    except Exception as e:
        logger.error(f"Error getting actual columns: {e}")
        return {'all_columns': [], 'sample_data': {}}

def find_column_by_pattern(patterns):
    """Find column name by matching patterns"""
    actual_columns = get_actual_columns()['all_columns']
    
    for pattern in patterns:
        for col in actual_columns:
            if pattern.lower() in col.lower():
                return col
    return None

def get_column_names():
    """Get the actual column names for state, locno, and plantno"""
    return {
        'state_column': find_column_by_pattern(['state', 'statename', 'region', 'area']),
        'locno_column': find_column_by_pattern(['locno', 'location', 'loc', 'site', 'station']),
        'plantno_column': find_column_by_pattern(['plantno', 'plant', 'unit', 'machine', 'device'])
    }

def get_dropdown_data(state_filter=None, locno_filter=None):
    """Get data for dropdown filters with dependencies"""
    try:
        conn = get_db_connection()
        columns = get_column_names()
        
        dropdown_data = {
            'states': [],
            'locnos': [],
            'plantnos': []
        }
        
        # Get states (always all states)
        if columns['state_column']:
            states_query = f"SELECT DISTINCT {columns['state_column']} FROM weatherdata2 WHERE {columns['state_column']} IS NOT NULL ORDER BY {columns['state_column']}"
            states_df = pd.read_sql(states_query, conn)
            dropdown_data['states'] = [str(x) for x in states_df[columns['state_column']].tolist() if x]
        
        # Get locnos based on state filter
        if columns['locno_column']:
            if state_filter and state_filter != 'all' and columns['state_column']:
                locno_query = f"""
                    SELECT DISTINCT {columns['locno_column']} 
                    FROM weatherdata2 
                    WHERE {columns['locno_column']} IS NOT NULL 
                    AND {columns['state_column']} = ? 
                    ORDER BY {columns['locno_column']}
                """
                locno_df = pd.read_sql(locno_query, conn, params=[state_filter])
            else:
                locno_query = f"SELECT DISTINCT {columns['locno_column']} FROM weatherdata2 WHERE {columns['locno_column']} IS NOT NULL ORDER BY {columns['locno_column']}"
                locno_df = pd.read_sql(locno_query, conn)
            dropdown_data['locnos'] = [str(x) for x in locno_df[columns['locno_column']].tolist() if x]
        
        # Get plantnos based on state and locno filters
        if columns['plantno_column']:
            conditions = []
            params = []
            
            if state_filter and state_filter != 'all' and columns['state_column']:
                conditions.append(f"{columns['state_column']} = ?")
                params.append(state_filter)
            
            if locno_filter and locno_filter != 'all' and columns['locno_column']:
                conditions.append(f"{columns['locno_column']} = ?")
                params.append(locno_filter)
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            plantno_query = f"""
                SELECT DISTINCT {columns['plantno_column']} 
                FROM weatherdata2 
                WHERE {columns['plantno_column']} IS NOT NULL 
                AND {where_clause}
                ORDER BY {columns['plantno_column']}
            """
            plantno_df = pd.read_sql(plantno_query, conn, params=params)
            dropdown_data['plantnos'] = [str(x) for x in plantno_df[columns['plantno_column']].tolist() if x]
        
        conn.close()
        return dropdown_data
        
    except Exception as e:
        logger.error(f"Failed to fetch dropdown data: {e}")
        return {'states': [], 'locnos': [], 'plantnos': []}

def get_all_weather_data():
    """Fetch top 2000 weather records"""
    try:
        conn = get_db_connection()
        query = "SELECT TOP 20000 * FROM weatherdata2 ORDER BY CreatedOn DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"Retrieved {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return pd.DataFrame()

def get_filtered_data(filters):
    """Get filtered data based on selections"""
    try:
        conn = get_db_connection()
        columns = get_column_names()
        
        where_conditions = []
        params = []
        
        if filters.get('state') and filters['state'] != 'all' and columns['state_column']:
            where_conditions.append(f"{columns['state_column']} = ?")
            params.append(filters['state'])
        
        if filters.get('locno') and filters['locno'] != 'all' and columns['locno_column']:
            where_conditions.append(f"{columns['locno_column']} = ?")
            params.append(filters['locno'])
        
        if filters.get('plantno') and filters['plantno'] != 'all' and columns['plantno_column']:
            where_conditions.append(f"{columns['plantno_column']} = ?")
            params.append(filters['plantno'])
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        query = f"SELECT TOP 1000 * FROM weatherdata2 WHERE {where_clause} ORDER BY CreatedOn DESC"
        
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Error in get_filtered_data: {e}")
        return pd.DataFrame()

def get_chart_summary_data(filters=None):
    """Get aggregated data for pie and bubble charts"""
    try:
        conn = get_db_connection()
        columns = get_column_names()
        
        # Build where conditions for filtered chart data
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('state') and filters['state'] != 'all' and columns['state_column']:
                where_conditions.append(f"{columns['state_column']} = ?")
                params.append(filters['state'])
            
            if filters.get('locno') and filters['locno'] != 'all' and columns['locno_column']:
                where_conditions.append(f"{columns['locno_column']} = ?")
                params.append(filters['locno'])
            
            if filters.get('plantno') and filters['plantno'] != 'all' and columns['plantno_column']:
                where_conditions.append(f"{columns['plantno_column']} = ?")
                params.append(filters['plantno'])
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        # Get state-wise average data for pie chart
        if columns['state_column']:
            state_query = f"""
                SELECT {columns['state_column']} as State, 
                       AVG(CAST(Temp AS FLOAT)) as avg_temp,
                       AVG(CAST(Humidity AS FLOAT)) as avg_humidity,
                       AVG(CAST(WindSpeed AS FLOAT)) as avg_windspeed,
                       COUNT(*) as record_count
                FROM weatherdata2 
                WHERE {columns['state_column']} IS NOT NULL 
                AND {where_clause}
                GROUP BY {columns['state_column']}
            """
            state_df = pd.read_sql(state_query, conn, params=params)
        else:
            state_df = pd.DataFrame()
        
        # Get location-wise data for bubble chart
        if columns['locno_column']:
            bubble_query = f"""
                SELECT {columns['locno_column']} as LOCNO,
                       AVG(CAST(Temp AS FLOAT)) as avg_temp,
                       AVG(CAST(Humidity AS FLOAT)) as avg_humidity,
                       AVG(CAST(WindSpeed AS FLOAT)) as avg_windspeed,
                       COUNT(*) as record_count
                FROM weatherdata2 
                WHERE {columns['locno_column']} IS NOT NULL 
                AND {where_clause}
                GROUP BY {columns['locno_column']}
            """
            bubble_df = pd.read_sql(bubble_query, conn, params=params)
        else:
            bubble_df = pd.DataFrame()
        
        # Get condition distribution
        conditions_query = f"""
            SELECT Conditions, COUNT(*) as count
            FROM weatherdata2 
            WHERE Conditions IS NOT NULL
            AND {where_clause}
            GROUP BY Conditions
            ORDER BY count DESC
        """
        conditions_df = pd.read_sql(conditions_query, conn, params=params)
        
        conn.close()
        
        return {
            'state_data': state_df.to_dict(orient='records'),
            'bubble_data': bubble_df.to_dict(orient='records'),
            'conditions_data': conditions_df.to_dict(orient='records')
        }
        
    except Exception as e:
        logger.error(f"Error getting chart summary: {e}")
        return {'state_data': [], 'bubble_data': [], 'conditions_data': []}

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

# === Dashboard API Routes ===
@app.route('/get_dropdown_options')
def get_dropdown_options():
    """Get dropdown options with optional filters"""
    try:
        state_filter = request.args.get('state', None)
        locno_filter = request.args.get('locno', None)
        
        data = get_dropdown_data(state_filter, locno_filter)
        return jsonify({
            'status': 'success',
            'data': data
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/get_data')
def get_data():
    """Get weather data with optional filters"""
    try:
        filters = {
            'state': request.args.get('state', 'all'),
            'locno': request.args.get('locno', 'all'),
            'plantno': request.args.get('plantno', 'all')
        }
        
        if filters['state'] == 'all' and filters['locno'] == 'all' and filters['plantno'] == 'all':
            df = get_all_weather_data()
        else:
            df = get_filtered_data(filters)
        
        if df.empty:
            return jsonify({'status': 'error', 'message': 'No data found'})
        
        # Get basic stats
        columns = get_column_names()
        stats = {
            'total_records': len(df),
            'unique_locations': len(set(df.get(columns['locno_column'], []) if columns['locno_column'] in df.columns else [])),
            'today_count': len([x for x in df.get('CreatedOn', []) if pd.to_datetime(x).date() == datetime.now().date()])
        }
        
        # Get chart summary data with same filters
        chart_summary = get_chart_summary_data(filters)
        
        return jsonify({
            'status': 'success',
            'data': df.to_dict(orient='records'),
            'stats': stats,
            'chart_summary': chart_summary,
            'filters_applied': filters
        })
        
    except Exception as e:
        logger.error(f"Error in get_data: {e}")
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/debug/columns')
def debug_columns():
    """Debug endpoint to see actual columns"""
    try:
        actual_data = get_actual_columns()
        columns = get_column_names()
        return jsonify({
            'status': 'success',
            'columns': actual_data['all_columns'],
            'sample_row': actual_data['sample_data'],
            'identified_columns': columns
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

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
    query = request.args.get("q", "")
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
    app.run(debug=True, host="0.0.0.0", port=6013)