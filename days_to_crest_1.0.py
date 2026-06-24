import os
import sys
import json
import asyncio
import aiohttp
import folium
from datetime import datetime, timezone, timedelta

# --- Configuration ---
BASE_API_URL = "https://api.water.noaa.gov/nwps/v1/gauges"
CACHE_FILE = "gauge_metadata_cache_all.json"
DASHBOARD_FILE = "index.html" 

MAX_CONCURRENT_REQUESTS = 75
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# --- Helper Functions ---
def has_valid_thresholds(categories):
    if not categories:
        return False
    for status_name in ["major", "moderate", "minor", "action"]:
        cat_data = categories.get(status_name)
        if cat_data is not None:
            threshold = cat_data.get("stage") if isinstance(cat_data, dict) else cat_data
            if threshold is not None and threshold > -50: 
                return True
    return False

def get_flood_status(stage, categories, record_stage=None):
    if stage is None or stage < -50:
        return "Normal"
    
    if record_stage is not None and record_stage > -50 and stage > record_stage:
        return "Record"
        
    for status_name in ["major", "moderate", "minor", "action"]:
        category_data = categories.get(status_name)
        if category_data is not None:
            threshold = category_data.get("stage") if isinstance(category_data, dict) else category_data
            if threshold is not None and threshold > -50 and stage >= threshold:
                return status_name.capitalize()
    return "Normal"

def get_status_color(status):
    colors = {
        "Record": "#00ccff",   
        "Major": "#cc33ff",    
        "Moderate": "#ff0000", 
        "Minor": "#ff9900",    
        "Action": "#ffff00",   
        "Normal": "#00ff00",   
    }
    return colors.get(status, "#cccccc") 

def get_crest_info(forecast_data):
    if not forecast_data:
        return "No forecast available", -1.0, "", None, False

    max_stage = -1.0
    crest_time_str = None
    max_idx = -1
    
    for idx, entry in enumerate(forecast_data):
        stage = entry.get("primary")
        if stage is not None and stage > max_stage:
            max_stage = stage
            crest_time_str = entry.get("validTime")
            max_idx = idx
            
    if not crest_time_str:
        return "Unknown", max_stage, "", None, False
        
    is_uncaptured = (max_idx == len(forecast_data) - 1)
        
    try:
        crest_dt = datetime.fromisoformat(crest_time_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        
        time_formatted = crest_dt.strftime('%m-%d %H:%M UTC')
        crest_day_str = crest_dt.strftime('%a %p') 
        
        shifted_now = now - timedelta(hours=12)
        shifted_crest = crest_dt - timedelta(hours=12)
        delta_days = (shifted_crest.date() - shifted_now.date()).days
        day_num = str(max(1, delta_days + 1)) 
        
        if is_uncaptured:
            day_num = f"{day_num}+"
            crest_str = f"Forecast Rising: {max_stage}ft+ on {crest_day_str} ({time_formatted}) [Uncapped]"
        else:
            crest_str = f"Forecast Crest: {max_stage}ft on {crest_day_str} ({time_formatted})"
            
        return crest_str, max_stage, day_num, crest_dt, is_uncaptured 
    except Exception:
        day_str = "7+" if is_uncaptured else "7"
        return f"Forecast Crest: {max_stage}ft", max_stage, day_str, None, is_uncaptured

def get_stage_at_time(timeseries_data, target_dt):
    if not timeseries_data: return None
    try:
        first_dt = datetime.fromisoformat(timeseries_data[0]["validTime"].replace('Z', '+00:00'))
        last_dt = datetime.fromisoformat(timeseries_data[-1]["validTime"].replace('Z', '+00:00'))
        
        if target_dt < first_dt:
            return timeseries_data[0].get("primary")
            
        if target_dt > last_dt:
            return None
            
        for i in range(len(timeseries_data) - 1):
            t1 = datetime.fromisoformat(timeseries_data[i]["validTime"].replace('Z', '+00:00'))
            t2 = datetime.fromisoformat(timeseries_data[i+1]["validTime"].replace('Z', '+00:00'))
            if t1 <= target_dt <= t2:
                s1 = timeseries_data[i].get("primary")
                s2 = timeseries_data[i+1].get("primary")
                if s1 is None or s2 is None: return None
                if t1 == t2: return s1
                
                ratio = (target_dt - t1).total_seconds() / (t2 - t1).total_seconds()
                return s1 + ratio * (s2 - s1)
    except Exception:
        pass
    return None

# --- Core Asynchronous Logic ---
async def fetch_all_gauge_lids(session, retries=5):
    print("Discovering all NWPS gauges...")
    for attempt in range(retries):
        try:
            async with session.get(BASE_API_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    if "gauges" in data:
                        return [g.get("lid") for g in data["gauges"] if "lid" in g]
                else:
                    print(f"NOAA API returned status {response.status}. Retrying...")
        except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError, Exception) as e:
            print(f"Discovery attempt {attempt + 1} failed: {e}")
            if attempt == retries - 1:
                print("Max retries reached. NOAA API is unresponsive.")
                return []
            await asyncio.sleep(5) 
    return []

async def fetch_with_retries(session, url, lid, retries=3):
    async with semaphore:
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return lid, data
                    return lid, None
            except (aiohttp.ClientPayloadError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError):
                if attempt == retries - 1:
                    return lid, None
                await asyncio.sleep(1) 

async def fetch_metadata(session, lid):
    url = f"{BASE_API_URL}/{lid}"
    return await fetch_with_retries(session, url, lid)

async def fetch_stageflow(session, lid):
    url = f"{BASE_API_URL}/{lid}/stageflow"
    return await fetch_with_retries(session, url, lid)

async def build_metadata_cache(session):
    if os.path.exists(CACHE_FILE):
        print("Loading gauge metadata from cache (fast!)...")
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
            
    target_gauges = await fetch_all_gauge_lids(session)
    if not target_gauges:
        return {}

    print(f"Fetching metadata for {len(target_gauges)} gauges...")
    metadata = {}
    tasks = [fetch_metadata(session, lid) for lid in target_gauges]
    
    total_meta = len(tasks)
    completed_meta = 0
    for f in asyncio.as_completed(tasks):
        lid, data = await f
        completed_meta += 1
        if completed_meta % 1000 == 0:
            print(f"  ... Parsed metadata for {completed_meta} / {total_meta} gauges")
        if data and "latitude" in data and "longitude" in data:
            metadata[lid] = data
                
    with open(CACHE_FILE, "w") as f:
        json.dump(metadata, f)
        
    return metadata

async def generate_dashboard():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=3600, connect=60, sock_read=60) 
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        metadata = await build_metadata_cache(session)
        
        valid_lids = []
        for lid, meta in metadata.items():
            if lid in ["ESLN8", "DCBN8"]: 
                continue
            flood_categories = meta.get("flood", {}).get("categories", {})
            if has_valid_thresholds(flood_categories):
                valid_lids.append(lid)
                
        print(f"Filtered to {len(valid_lids)} active gauges with flood thresholds.")
        print(f"Fetching real-time stage/flow updates for {len(valid_lids)} gauges...")
        
        stageflow_data = {}
        tasks = [fetch_stageflow(session, lid) for lid in valid_lids]
        
        total_tasks = len(tasks)
        completed = 0
        
        for f in asyncio.as_completed(tasks):
            lid, data = await f
            completed += 1
            if completed % 500 == 0:
                print(f"  ... Downloaded {completed} / {total_tasks} gauges")
            if data:
                stageflow_data[lid] = data

    print("Building Map Dashboard...")
    m = folium.Map(location=[39.8, -98.5], zoom_start=4, tiles=None)
    folium.TileLayer('CartoDB positron', name='Light Mode').add_to(m)
    folium.TileLayer('CartoDB dark_matter', name='Dark Mode', opacity=0.85, show=False).add_to(m)
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri World Imagery',
        name='Satellite Mode',
        show=False
    ).add_to(m)
    folium.TileLayer('OpenStreetMap', name='Open Street Map', show=False).add_to(m)

    txt_outline = "text-shadow: -1px -1px 0 #888, 1px -1px 0 #888, -1px 1px 0 #888, 1px 1px 0 #888;"

    fg_record = folium.FeatureGroup(name=f"<span style='color: #00ccff; font-weight: bold; {txt_outline}'>&#9650; Record Flood</span>")
    fg_major = folium.FeatureGroup(name=f"<span style='color: #cc33ff; font-weight: bold; {txt_outline}'>&#9650; Major Flood</span>")
    fg_moderate = folium.FeatureGroup(name=f"<span style='color: #ff0000; font-weight: bold; {txt_outline}'>&#9650; Moderate Flood</span>")
    fg_minor = folium.FeatureGroup(name=f"<span style='color: #ff9900; font-weight: bold; {txt_outline}'>&#9650; Minor Flood</span>")
    fg_action = folium.FeatureGroup(name=f"<span style='color: #ffff00; font-weight: bold; {txt_outline}'>&#9650; Action Stage</span>", show=False)
    
    layer_map = {
        "Record": fg_record,
        "Major": fg_major,
        "Moderate": fg_moderate,
        "Minor": fg_minor,
        "Action": fg_action
    }

    base_slider_data = [] 

    now_utc = datetime.now(timezone.utc)
    hour_bucket = (now_utc.hour // 6) * 6
    timeline_start_dt = now_utc.replace(hour=hour_bucket, minute=0, second=0, microsecond=0)
    
    forecast_timeline_labels = {}
    for i in range(-28, 29):
        t = timeline_start_dt + timedelta(hours=6*i)
        forecast_timeline_labels[str(i)] = t.strftime('%b %d %H:%M UTC')

    lookback_dates = {}
    for i in range(-30, 1):
        d = now_utc + timedelta(days=i)
        lookback_dates[str(i)] = d.strftime('%b %d')

    severityMapDict = {"Record": 5, "Major": 4, "Moderate": 3, "Minor": 2, "Action": 1, "Normal": 0}

    for lid in valid_lids:
        meta = metadata.get(lid, {})
        flood_categories = meta.get("flood", {}).get("categories", {})
        lat = meta.get("latitude")
        lon = meta.get("longitude")
        sf_data = stageflow_data.get(lid, {})
        
        raw_observed_array = sf_data.get("observed", {}).get("data", [])
        forecast_array = sf_data.get("forecast", {}).get("data", [])
        
        cleaned_observed = []
        valid_obs = [obs for obs in raw_observed_array if obs.get("primary") is not None and obs.get("primary") > -50]
        
        for i, obs in enumerate(valid_obs):
            val = obs.get("primary")
            is_spike = False
            
            if 2 <= i <= len(valid_obs) - 3:
                prev_min = min(valid_obs[i-1].get("primary"), valid_obs[i-2].get("primary"))
                next_min = min(valid_obs[i+1].get("primary"), valid_obs[i+2].get("primary"))
                if (val - prev_min > 2.0) and (val - next_min > 2.0):
                    is_spike = True
            elif 0 < i < len(valid_obs) - 1:
                prev_val = valid_obs[i-1].get("primary")
                next_val = valid_obs[i+1].get("primary")
                if (val - prev_val > 2.0) and (val - next_val > 2.0):
                    is_spike = True
                    
            if not is_spike:
                cleaned_observed.append(obs)
                
        observed_array = cleaned_observed
        
        combined_timeseries = []
        for item in observed_array + forecast_array:
            if item.get("validTime") and item.get("primary") is not None and item.get("primary") > -50:
                combined_timeseries.append(item)
        combined_timeseries.sort(key=lambda x: datetime.fromisoformat(x["validTime"].replace('Z', '+00:00')))

        record_stage = None
        rec_cat = flood_categories.get("record")
        if rec_cat is not None:
            record_stage = rec_cat.get("stage") if isinstance(rec_cat, dict) else rec_cat
            
        if record_stage is None or record_stage <= -50:
            peaks = meta.get("historical", {}).get("peaks", [])
            max_p = -100.0
            for p in peaks:
                stg = p.get("stage")
                if stg is not None and stg > max_p:
                    max_p = stg
            if max_p > -50:
                record_stage = max_p

        current_stage = None
        past_stages = {}
        past_statuses = {}
        max_obs_30d = -100.0
        past_30d_dt = now_utc - timedelta(days=30)

        if observed_array:
            for obs in reversed(observed_array):
                val = obs.get("primary")
                time_str = obs.get("validTime")
                if val is not None and val > -50 and time_str:
                    try:
                        obs_dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                        if (now_utc - obs_dt).total_seconds() <= 6 * 3600:
                            current_stage = val
                            break
                    except Exception:
                        pass
            
            for obs in observed_array:
                val = obs.get("primary")
                time_str = obs.get("validTime")
                if val is not None and val > -50 and time_str:
                    try:
                        obs_dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                        if obs_dt >= past_30d_dt:
                            if val > max_obs_30d:
                                max_obs_30d = val
                            
                            days_ago_offset = (now_utc.date() - obs_dt.date()).days
                            if days_ago_offset >= 0 and days_ago_offset <= 30:
                                if days_ago_offset not in past_stages or val > past_stages[days_ago_offset]:
                                    past_stages[days_ago_offset] = round(val, 2)
                                    past_statuses[days_ago_offset] = get_flood_status(val, flood_categories, record_stage)
                    except Exception:
                        pass
                            
        current_status = get_flood_status(current_stage, flood_categories, record_stage)
        
        forecast_max = -1.0
        max_idx = -1
        for idx, entry in enumerate(forecast_array):
            stage = entry.get("primary")
            if stage is not None and stage > forecast_max:
                forecast_max = stage
                max_idx = idx

        first_fx_stage = forecast_array[0].get("primary") if forecast_array else None
        
        is_crested_overall = False
        if first_fx_stage is not None and first_fx_stage >= forecast_max - 0.05:
            is_crested_overall = True

        if is_crested_overall and first_fx_stage is not None:
            forecast_status = get_flood_status(first_fx_stage, flood_categories, record_stage)
            crest_str = f"Status: Crested & Receding (Peak: {round(first_fx_stage, 2)}ft)"
            crest_day_num = "1"
        else:
            forecast_status = get_flood_status(forecast_max, flood_categories, record_stage)
            crest_str, _, crest_day_num, _, _ = get_crest_info(forecast_array)
        
        max_lookback_status = "Normal"
        for offset_val in past_statuses.values():
            if offset_val == "Record": max_lookback_status = "Record"
            elif offset_val == "Major" and max_lookback_status not in ["Record"]: max_lookback_status = "Major"
            elif offset_val == "Moderate" and max_lookback_status not in ["Record", "Major"]: max_lookback_status = "Moderate"
            elif offset_val == "Minor" and max_lookback_status not in ["Record", "Major", "Moderate"]: max_lookback_status = "Minor"
            elif offset_val == "Action" and max_lookback_status == "Normal": max_lookback_status = "Action"
            
        if current_status == "Normal" and forecast_status == "Normal" and max_lookback_status == "Normal":
            continue
                
        frontend_ts = [{"t": x["validTime"], "v": round(x["primary"], 2)} for x in combined_timeseries]
        
        t_levels = {}
        for s_lbl in ["major", "moderate", "minor", "action"]:
            c_data = flood_categories.get(s_lbl)
            val = c_data.get("stage") if isinstance(c_data, dict) else c_data
            if val is not None and val > -50:
                t_levels[s_lbl] = val

        obs_color = get_status_color(current_status)
        fx_color = get_status_color(forecast_status)
        
        if is_crested_overall:
            shape_svg = f"""
            <svg width="28" height="28" viewBox="0 0 28 28" style="display: block; margin: 0 auto;">
                <polygon points="1,3 27,3 14,25" fill="{fx_color}" stroke="black" stroke-width="1.5"/>
                <circle cx="14" cy="10" r="7.5" fill="{obs_color}" stroke="black" stroke-width="1"/>
            </svg>
            """
        else:
            shape_svg = f"""
            <svg width="28" height="28" viewBox="0 0 28 28" style="display: block; margin: 0 auto;">
                <polygon points="14,2 27,24 1,24" fill="{fx_color}" stroke="black" stroke-width="1.5"/>
                <circle cx="14" cy="18" r="7.5" fill="{obs_color}" stroke="black" stroke-width="1"/>
                <text x="14" y="22" font-size="11" font-family="Arial" font-weight="bold" text-anchor="middle" fill="black">{crest_day_num}</text>
            </svg>
            """
            
        icon_html = f"""
        <div style="text-align: center; width: 34px;">
            {shape_svg}
        </div>
        """
        
        placeholder_popup = f"<div class='dynamic-popup' data-lid='{lid}'>Loading Map Engine...</div>"
        
        display_status = forecast_status if forecast_status != "Normal" else current_status
        has_forecast = len(forecast_array) > 0
        
        show_on_static = True
        
        # --- NEW: Filter out Observation-Only gauges from Static Mode ---
        if display_status == "Normal" or not has_forecast:
            show_on_static = False
            # Safely capture the highest past max state so it still renders perfectly in Lookback Mode
            if display_status == "Normal":
                display_status = max_lookback_status
            
        if show_on_static:
            static_z_offset = severityMapDict.get(display_status, 0) * 1000
            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(placeholder_popup, max_width=500),
                icon=folium.DivIcon(html=icon_html, icon_size=(34, 34), icon_anchor=(17, 17)),
                z_index_offset=static_z_offset
            ).add_to(layer_map[display_status])
        
        base_slider_data.append({
            "id": lid,
            "la": round(lat, 5),
            "lo": round(lon, 5),
            "color": fx_color,
            "past_stages": past_stages,
            "past_statuses": past_statuses,
            "m30": round(max_obs_30d, 2) if max_obs_30d > -50 else None,
            "day_str": str(crest_day_num), 
            "has_forecast": has_forecast, 
            "current_status": current_status,
            "forecast_status": forecast_status,
            "display_status": display_status, 
            "is_crested": is_crested_overall, 
            "show_on_static": show_on_static,
            "thresholds": t_levels,
            "record": record_stage,
            "timeseries": frontend_ts,
            "cstr": crest_str,
            "c_stg": round(current_stage, 2) if current_stage is not None else None
        })

    fg_action.add_to(m)
    fg_minor.add_to(m)
    fg_moderate.add_to(m)
    fg_major.add_to(m)
    fg_record.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    
    shifted_now = now_utc - timedelta(hours=12)
    base_date = shifted_now.date()
    day_bounds = {}
    for i in range(1, 8):
        start_dt = datetime(base_date.year, base_date.month, base_date.day, 12, 0, tzinfo=timezone.utc) + timedelta(days=i-1)
        end_dt = start_dt + timedelta(days=1)
        day_bounds[str(i)] = f"{start_dt.strftime('%b %d %H:%M')} UTC - {end_dt.strftime('%b %d %H:%M')} UTC"

    update_time_str = now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')

    custom_ui_html = f"""
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1"></script>

    <style>
        .leaflet-control-layers {{
            font-family: 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif !important;
            font-size: 16px !important; 
            line-height: 1.5 !important;
            padding: 12px 18px !important;
            border-radius: 8px !important;
            box-shadow: 2px 2px 10px rgba(0,0,0,0.5) !important;
            background: rgba(30, 30, 30, 0.95) !important;
            color: white !important;
            border: 1px solid #555 !important;
        }}
        .leaflet-control-layers-expanded {{
            background: rgba(30, 30, 30, 0.95) !important;
            color: white !important;
        }}
        .leaflet-control-layers label {{
            font-size: 18px !important; 
            margin-bottom: 6px !important;
            cursor: pointer;
            display: flex;
            align-items: center;
        }}
        .leaflet-control-layers-separator {{
            margin: 12px 0 !important;
            border-top: 1px solid #555 !important;
        }}
        .leaflet-control-layers input[type="checkbox"],
        .leaflet-control-layers input[type="radio"] {{
            width: 20px;
            height: 20px;
            margin-right: 10px;
            cursor: pointer;
        }}
        .range-container {{ text-align: left; margin-bottom: 5px; }}
        .range-container label {{ font-size: 14px; display: inline-block; width: 45px; font-weight: bold; }}
        .range-container input {{ width: calc(100% - 55px); vertical-align: middle; }}
        
        #forecastInput::-webkit-slider-runnable-track {{
            background: linear-gradient(to right, #444 0%, #444 50%, #555 50%, #1e3d59 50%, #1e3d59 100%) !important;
            border-radius: 4px;
            height: 8px;
        }}
        #forecastInput::-moz-range-track {{
            background: linear-gradient(to right, #444 0%, #444 50%, #555 50%, #1e3d59 50%, #1e3d59 100%) !important;
            border-radius: 4px;
            height: 8px;
        }}
    </style>

    <div style="position: fixed; top: 15px; left: 50%; transform: translateX(-50%); z-index: 9999; display: flex; gap: 8px; background: rgba(30, 30, 30, 0.85); padding: 10px 12px; border-radius: 8px; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5);">
        <div id="btnStaticMode" style="background: #555; color: white; padding: 10px 14px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 14px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center; white-space: nowrap;">
            &#128506; Static
        </div>
        <div id="btnLookback" style="background: #222; color: white; padding: 10px 14px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 14px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center; white-space: nowrap;">
            &#128260; Past Max
        </div>
        <div id="btnCrestSlider" style="background: #222; color: white; padding: 10px 14px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 14px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center; white-space: nowrap;">
            &#9202; Crests
        </div>
        <div id="btnCrestHeatmap" style="background: #222; color: white; padding: 10px 14px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 14px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center; white-space: nowrap;">
            &#128396; Heatmap
        </div>
        <div id="btnForecastSlider" style="background: #222; color: white; padding: 10px 14px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 14px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center; white-space: nowrap;">
            &#128200; Timeline
        </div>
    </div>
    
    <div style="position: fixed; top: 80px; left: 60px; z-index: 9999; background: rgba(30, 30, 30, 0.95); color: white; padding: 15px; border-radius: 4px; border: 1px solid #555; font-family: 'Segoe UI', Arial, sans-serif; font-size: 15px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); line-height: 1.5; width: 310px;">
        <div style="font-size: 16px; font-weight: bold; margin-bottom: 4px;">Current View Summary</div>
        <hr style="margin: 6px 0; border-color: #555;">
        Observed Flooding: <span id="count-obs" style="font-weight: bold; color: #ffcc00;">0</span><br>
        Forecast Only: <span id="count-fxonly" style="font-weight: bold; color: #ffcc00;">0</span><br>
        <hr style="margin: 6px 0; border-color: #555;">
        <div style="font-weight: bold; margin-bottom: 3px;">Forecast Breakdown:</div>
        Record: <span id="count-record" style="font-weight: bold; color: #00ccff;">0</span> | 
        Major: <span id="count-major" style="font-weight: bold; color: #cc33ff;">0</span><br>
        Moderate: <span id="count-moderate" style="font-weight: bold; color: #ff0000;">0</span> | 
        Minor: <span id="count-minor" style="font-weight: bold; color: #ff9900;">0</span><br>
        Action: <span id="count-action" style="font-weight: bold; color: #ffff00;">0</span><br>
        <hr style="margin: 6px 0; border-color: #555;">
        
        <div style="font-weight: bold; margin-bottom: 3px;">Crest Breakdown (Days 1-7):</div>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2px 10px; font-size: 14px;">
            <div>Day 1: <span id="count-day1" style="font-weight: bold; color: #00ffcc;">0</span></div>
            <div>Day 2: <span id="count-day2" style="font-weight: bold; color: #00ffcc;">0</span></div>
            <div>Day 3: <span id="count-day3" style="font-weight: bold; color: #00ffcc;">0</span></div>
            <div>Day 4: <span id="count-day4" style="font-weight: bold; color: #00ffcc;">0</span></div>
            <div>Day 5: <span id="count-day5" style="font-weight: bold; color: #00ffcc;">0</span></div>
            <div>Day 6: <span id="count-day6" style="font-weight: bold; color: #00ffcc;">0</span></div>
            <div>Day 7: <span id="count-day7" style="font-weight: bold; color: #00ffcc;">0</span></div>
        </div>
        
        <div id="dynamicCrestCount" style="display: none; margin-top: 8px; padding-top: 6px; border-top: 1px dashed #555;">
            Cresting on Day <span id="count-day-label">1</span>: <span id="count-day" style="font-weight: bold; color: #00ffcc;">0</span>
        </div>
    </div>

    <div id="heatmapLegendUI" style="position: fixed; bottom: 100px; right: 20px; z-index: 9999; background: rgba(30, 30, 30, 0.95); color: white; padding: 12px; border-radius: 4px; border: 1px solid #555; font-family: 'Segoe UI', Arial, sans-serif; font-size: 13px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); width: 170px; display: none;">
        <div style="font-weight: bold; margin-bottom: 8px; text-align: center;">Crest Timeline Horizon</div>
        <div style="display: flex; flex-direction: column; gap: 5px;">
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #2667FF; border-radius: 50%; border: 1px solid #111;"></div> Day 1 (Immediate)</div>
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #5EA4FF; border-radius: 50%; border: 1px solid #111;"></div> Day 2</div>
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #9ACEE2; border-radius: 50%; border: 1px solid #111;"></div> Day 3</div>
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #C2D2B5; border-radius: 50%; border: 1px solid #111;"></div> Day 4</div>
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #E3D398; border-radius: 50%; border: 1px solid #111;"></div> Day 5</div>
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #F1CB84; border-radius: 50%; border: 1px solid #111;"></div> Day 6</div>
            <div style="display: flex; align-items: center; gap: 8px;"><div style="width: 14px; height: 14px; background: #FFC371; border-radius: 50%; border: 1px solid #111;"></div> Day 7 / 7+ (Distant)</div>
        </div>
    </div>

    <div style="position: fixed; bottom: 25px; left: 25px; z-index: 9999; background: rgba(30, 30, 30, 0.9); color: #aaa; padding: 8px 12px; border-radius: 4px; border: 1px solid #444; font-family: 'Segoe UI', Arial, sans-serif; font-size: 12px; box-shadow: 2px 2px 5px rgba(0,0,0,0.4); pointer-events: none;">
        Dashboard Updated: <span style="color: #00ffcc; font-weight: bold;">{update_time_str}</span>
    </div>
    
    <div id="lookbackSliderUI" style="position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%); z-index: 9999; background: rgba(30, 30, 30, 0.95); padding: 15px 25px; border-radius: 8px; display: none; color: white; font-family: 'Segoe UI', Arial, sans-serif; text-align: center; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); width: 400px;">
        <h4 style="margin: 0 0 10px 0; font-size: 16px;">Past Max Lookback Window</h4>
        <div style="display: flex; justify-content: space-between; font-size: 13px; color: #00ffcc; margin-bottom: 8px; padding: 0 5px;">
            <span id="lookbackStartLbl">Start Date</span>
            <span id="lookbackEndLbl">End Date</span>
        </div>
        <div class="range-container">
            <label>Start:</label><input type="range" id="lookbackStartInput" min="-30" max="0" value="-7" step="1">
        </div>
        <div class="range-container">
            <label>End:</label><input type="range" id="lookbackEndInput" min="-30" max="0" value="0" step="1">
        </div>
    </div>

    <div id="crestSliderUI" style="position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%); z-index: 9999; background: rgba(30, 30, 30, 0.95); padding: 15px 25px; border-radius: 8px; display: none; color: white; font-family: 'Segoe UI', Arial, sans-serif; text-align: center; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); width: 340px;">
        <h4 style="margin: 0 0 10px 0; font-size: 16px;">Forecast Crest Timeline</h4>
        <input type="range" id="crestInput" min="1" max="7" value="1" step="1" style="width: 100%; cursor: pointer;">
        <div id="crestLabel" style="margin-top: 10px; font-weight: bold; font-size: 18px;">Day 1</div>
        <div id="crestDateRange" style="font-size: 13px; color: #00ffcc; margin-top: 5px;"></div>
    </div>
    
    <div id="forecastSliderUI" style="position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%); z-index: 9999; background: rgba(30, 30, 30, 0.95); padding: 15px 25px; border-radius: 8px; display: none; color: white; font-family: 'Segoe UI', Arial, sans-serif; text-align: center; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); width: 380px;">
        <h4 style="margin: 0 0 10px 0; font-size: 16px;">Past & Future Seamless Timeline (6-Hr)</h4>
        <input type="range" id="forecastInput" min="-28" max="28" value="0" step="1" style="width: 100%; cursor: pointer;">
        <div id="forecastLabel" style="margin-top: 10px; font-weight: bold; font-size: 18px;">Initializing...</div>
    </div>
    
    <script>
    window.addEventListener("load", function() {{
        var map = {m.get_name()}; 
        var baseData = {json.dumps(base_slider_data, separators=(',', ':'))};
        var dayBounds = {json.dumps(day_bounds, separators=(',', ':'))};
        var fxLabels = {json.dumps(forecast_timeline_labels, separators=(',', ':'))};
        var lookbackDates = {json.dumps(lookback_dates, separators=(',', ':'))};
        var timelineStartDt = new Date("{timeline_start_dt.isoformat()}");
        
        map.createPane('sliderTimelinePane');
        map.getPane('sliderTimelinePane').style.zIndex = 650;
        
        var dynamicLayer = L.layerGroup().addTo(map);
        
        var btnStatic = document.getElementById('btnStaticMode');
        var btnLookback = document.getElementById('btnLookback');
        var btnCrest = document.getElementById('btnCrestSlider');
        var btnHeatmap = document.getElementById('btnCrestHeatmap');
        var btnForecast = document.getElementById('btnForecastSlider');
        
        var uiLookback = document.getElementById('lookbackSliderUI');
        var uiCrest = document.getElementById('crestSliderUI');
        var uiForecast = document.getElementById('forecastSliderUI');
        var uiCrestCount = document.getElementById('dynamicCrestCount');
        var uiHeatLegend = document.getElementById('heatmapLegendUI');
        
        var inputLookbackStart = document.getElementById('lookbackStartInput');
        var inputLookbackEnd = document.getElementById('lookbackEndInput');
        var inputCrest = document.getElementById('crestInput');
        var inputForecast = document.getElementById('forecastInput');
        
        var lblLookbackStart = document.getElementById('lookbackStartLbl');
        var lblLookbackEnd = document.getElementById('lookbackEndLbl');
        var lblCrest = document.getElementById('crestLabel');
        var lblCrestDates = document.getElementById('crestDateRange');
        var lblForecast = document.getElementById('forecastLabel');
        
        var markerPane = document.querySelector('.leaflet-marker-pane');
        var activeMode = 'base'; 
        
        var heatColors = {{
            "1": "#2667FF",
            "2": "#5EA4FF",
            "3": "#9ACEE2",
            "4": "#C2D2B5",
            "5": "#E3D398",
            "6": "#F1CB84",
            "7": "#FFC371",
            "7+": "#FFC371"
        }};
        
        var activeStatuses = {{ "Record": true, "Major": true, "Moderate": true, "Minor": true, "Action": false }};
        var severityMapDict = {{ "Record": 5, "Major": 4, "Moderate": 3, "Minor": 2, "Action": 1, "Normal": 0 }};
        var colorMap = {{ "Record": "#00ccff", "Major": "#cc33ff", "Moderate": "#ff0000", "Minor": "#ff9900", "Action": "#ffff00", "Normal": "#00ff00" }};
        
        var activeChartInstance = null;
        var renderTimer = null; 

        function getStageAtTime(timeseries, targetDt) {{
            if (!timeseries || timeseries.length === 0) return null;
            var firstDt = new Date(timeseries[0].t).getTime();
            var lastDt = new Date(timeseries[timeseries.length - 1].t).getTime();
            var tarMs = targetDt.getTime();
            
            if (tarMs < firstDt) return timeseries[0].v;
            if (tarMs > lastDt) return null;
            
            for (var i = 0; i < timeseries.length - 1; i++) {{
                var t1 = new Date(timeseries[i].t).getTime();
                var t2 = new Date(timeseries[i+1].t).getTime();
                if (tarMs >= t1 && tarMs <= t2) {{
                    var s1 = timeseries[i].v;
                    var s2 = timeseries[i+1].v;
                    if (s1 === null || s2 === null) return null;
                    if (t1 === t2) return s1;
                    
                    var ratio = (tarMs - t1) / (t2 - t1);
                    return s1 + ratio * (s2 - s1);
                }}
            }}
            return null;
        }}

        function getFloodStatusJS(stage, thresholds, recordStage) {{
            if (stage === null) return "Normal";
            if (recordStage !== null && recordStage > -50 && stage > recordStage) return "Record";
            if (thresholds.major !== undefined && stage >= thresholds.major) return "Major";
            if (thresholds.moderate !== undefined && stage >= thresholds.moderate) return "Moderate";
            if (thresholds.minor !== undefined && stage >= thresholds.minor) return "Minor";
            if (thresholds.action !== undefined && stage >= thresholds.action) return "Action";
            return "Normal";
        }}

        function generatePopupHTML(lid, mode, stepVal) {{
            var gData = baseData.find(d => d.id === lid);
            if (!gData) return "Error loading data.";
            
            var imgUrl = `https://water.noaa.gov/resources/hydrographs/${{lid.toLowerCase()}}_hg.png`;
            var nwpsUrl = `https://water.noaa.gov/gauges/${{lid}}`;
            
            var timeLabel = "";
            var valStr = "";
            var injectedSubtext = "";
            
            if (mode === 'base' || mode === 'heatmap' || mode === 'crest') {{
                timeLabel = "Current Stage";
                valStr = gData.c_stg !== null ? gData.c_stg.toFixed(2) + 'ft' : 'N/A';
                injectedSubtext = `<b>Current Stage:</b> ${{valStr}}<br><hr style="margin: 5px 0;"><b>Past 30-Day Max:</b> ${{gData.m30 !== null ? gData.m30.toFixed(2) + 'ft' : 'N/A'}}<br>`;
            }} else if (mode === 'lookback') {{
                var s_offset = Math.abs(parseInt(inputLookbackEnd.value));
                var e_offset = Math.abs(parseInt(inputLookbackStart.value));
                var max_val = -100.0;
                for(let j = s_offset; j <= e_offset; j++) {{
                    if(gData.past_stages[j] !== undefined && gData.past_stages[j] > max_val) {{
                        max_val = gData.past_stages[j];
                    }}
                }}
                valStr = max_val > -50 ? max_val.toFixed(2) + 'ft' : 'N/A';
                injectedSubtext = `<b>Current Stage:</b> ${{gData.c_stg !== null ? gData.c_stg.toFixed(2) + 'ft' : 'N/A'}}<br><hr style="margin: 5px 0;"><div style="font-size: 14px; color: #b30000; margin-bottom: 5px;"><b>Selected Window Max (${{lookbackDates[inputLookbackStart.value]}} to ${{lookbackDates[inputLookbackEnd.value]}}):</b> ${{valStr}}</div>`;
            }} else if (mode === 'forecast') {{
                timeLabel = stepVal < 0 ? "Past Stage" : "Forecast Stage";
                var targetDt = new Date(timelineStartDt.getTime() + (stepVal * 6 * 3600 * 1000));
                var sliceVal = getStageAtTime(gData.timeseries, targetDt);
                valStr = sliceVal !== null ? sliceVal.toFixed(2) + 'ft' : 'N/A';
                injectedSubtext = `<b>Interpolated ${{timeLabel}} at ${{fxLabels[stepVal.toString()]}}:</b> ${{valStr}}<br>`;
            }}
            
            return `
                <div class="dynamic-popup" style="width: 450px; font-family: Arial, sans-serif; color: black;" data-lid="${{lid}}" data-mode="${{mode}}" data-step="${{stepVal}}">
                    <h4 style="margin-bottom: 5px; margin-top: 0px;">Gauge: ${{lid}}</h4>
                    <div style="font-size: 14px; background: #eef2f5; padding: 6px; border-radius: 4px; margin-bottom: 5px; border: 1px solid #ccc;">
                        <b>${{gData.cstr}}</b>
                    </div>
                    ${{injectedSubtext}}
                    <hr style="margin: 5px 0;">
                    <div class="popupChartContainer">
                        <canvas class="gaugeChartCanvas" style="width:100%; height:200px; display:none;"></canvas>
                        <img class="noaaStaticImg" src="${{imgUrl}}" alt="NWPS Hydrograph" style="width:100%; border:1px solid #ccc; border-radius:4px; display:none;">
                    </div>
                    <br>
                    <a href="${{nwpsUrl}}" target="_blank" style="display: inline-block; margin-top: 8px; color: #0055ff; text-decoration: none; font-weight: bold;">&#128279; View Live NWPS Page</a>
                </div>
            `;
        }}

        map.on('popupopen', function(e) {{
            if (activeChartInstance) {{ activeChartInstance.destroy(); activeChartInstance = null; }}
            
            var node = e.popup._contentNode.querySelector('.dynamic-popup');
            if (!node) return;
            
            var lid = node.getAttribute('data-lid');
            var stepVal = activeMode === 'forecast' ? parseInt(inputForecast.value) : 0;
            
            e.popup.setContent(generatePopupHTML(lid, activeMode, stepVal));
            
            var container = e.popup._contentNode.querySelector('.dynamic-popup');
            var canvas = container.querySelector('.gaugeChartCanvas');
            var staticImg = container.querySelector('.noaaStaticImg');
            
            var gData = baseData.find(d => d.id === lid);
            if (!gData) return;

            var filteredData = [];
            var drawChart = true;

            if (activeMode === 'base' || activeMode === 'crest' || activeMode === 'heatmap') {{
                drawChart = false;
                if (canvas) canvas.style.display = 'none';
                if (staticImg) staticImg.style.display = 'block';
            }} 
            else if (activeMode === 'lookback') {{
                if (staticImg) staticImg.style.display = 'none';
                if (canvas) canvas.style.display = 'block';
                
                let startLimit = new Date(); startLimit.setDate(startLimit.getDate() + parseInt(inputLookbackStart.value)); startLimit.setHours(0,0,0,0);
                let endLimit = new Date(); endLimit.setDate(endLimit.getDate() + parseInt(inputLookbackEnd.value)); endLimit.setHours(23,59,59,999);
                
                filteredData = gData.timeseries.filter(pt => {{
                    let d = new Date(pt.t);
                    return d >= startLimit && d <= endLimit;
                }});
            }} 
            else if (activeMode === 'forecast') {{
                if (stepVal >= 0) {{
                    drawChart = false;
                    if (canvas) canvas.style.display = 'none';
                    if (staticImg) staticImg.style.display = 'block';
                }} else {{
                    if (staticImg) staticImg.style.display = 'none';
                    if (canvas) canvas.style.display = 'block';
                    
                    let baseAnchor = new Date(); 
                    let bucketHour = Math.floor(baseAnchor.getUTCHours() / 6) * 6;
                    baseAnchor.setUTCHours(bucketHour, 0,0,0);
                    baseAnchor.setHours(baseAnchor.getHours() + (stepVal * 6));

                    let winStart = new Date(baseAnchor.getTime() - (5 * 24 * 60 * 60 * 1000));
                    let winEnd = new Date(baseAnchor.getTime() + (5 * 24 * 60 * 60 * 1000));

                    filteredData = gData.timeseries.filter(pt => {{
                        let d = new Date(pt.t);
                        return d >= winStart && d <= winEnd;
                    }});
                }}
            }}

            if (!drawChart || !canvas) return;

            var labels = filteredData.map(pt => new Date(pt.t));
            var values = filteredData.map(pt => pt.v);
            var annotations = [];
            
            var colorsEnum = {{ "record": "#00ccff", "major": "#cc33ff", "moderate": "#ff0000", "minor": "#ff9900", "action": "#ffff00" }};
            
            if (gData.record && gData.record > -50) {{
                annotations.push({{
                    type: 'line',
                    id: 'line-record',
                    yMin: gData.record,
                    yMax: gData.record,
                    borderColor: colorsEnum['record'],
                    borderWidth: 2.0, 
                    label: {{ display: false }}
                }});
            }}
            for (let [lbl, thresholdVal] of Object.entries(gData.thresholds)) {{
                annotations.push({{
                    type: 'line',
                    id: 'line-' + lbl,
                    yMin: thresholdVal,
                    yMax: thresholdVal,
                    borderColor: colorsEnum[lbl],
                    borderWidth: 2.0, 
                    label: {{ display: false }}
                }});
            }}

            activeChartInstance = new Chart(canvas.getContext('2d'), {{
                type: 'line',
                data: {{
                    labels: labels,
                    datasets: [{{
                        label: 'Stage (ft)',
                        data: values,
                        borderColor: '#0055ff',
                        backgroundColor: 'rgba(0, 85, 255, 0.05)',
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: true,
                        tension: 0.1
                    }}]
                }},
                options: {{
                    responsive: false,
                    maintainAspectRatio: false,
                    scales: {{
                        x: {{
                            type: 'time',
                            time: {{ unit: 'day', displayFormats: {{ day: 'MMM d' }} }},
                            grid: {{ color: '#e5e5e5' }}
                        }},
                        y: {{ 
                            title: {{ display: true, text: 'Feet' }},
                            grid: {{ color: '#e5e5e5' }}
                        }}
                    }},
                    plugins: {{
                        legend: {{ display: false }},
                        autocolor: false,
                        annotation: {{ annotations: annotations }}
                    }}
                }}
            }});
        }});

        document.addEventListener('keydown', function(e) {{
            if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {{
                if (['forecast', 'crest', 'lookback'].includes(activeMode)) {{
                    e.preventDefault(); 
                    let changed = false;

                    if (activeMode === 'forecast') {{
                        let val = parseInt(inputForecast.value);
                        let step = (e.key === 'ArrowRight') ? 1 : -1;
                        let newVal = val + step;
                        if (newVal >= parseInt(inputForecast.min) && newVal <= parseInt(inputForecast.max)) {{
                            inputForecast.value = newVal;
                            lblForecast.innerText = fxLabels[newVal.toString()];
                            changed = true;
                        }}
                    }} 
                    else if (activeMode === 'crest') {{
                        let val = parseInt(inputCrest.value);
                        let step = (e.key === 'ArrowRight') ? 1 : -1;
                        let newVal = val + step;
                        if (newVal >= parseInt(inputCrest.min) && newVal <= parseInt(inputCrest.max)) {{
                            inputCrest.value = newVal;
                            lblCrest.innerText = "Day " + newVal;
                            if (dayBounds[newVal]) lblCrestDates.innerText = dayBounds[newVal];
                            changed = true;
                        }}
                    }}
                    else if (activeMode === 'lookback') {{
                        let sVal = parseInt(inputLookbackStart.value);
                        let eVal = parseInt(inputLookbackEnd.value);
                        let step = (e.key === 'ArrowRight') ? 1 : -1;
                        
                        let newSVal = sVal + step;
                        let newEVal = eVal + step;
                        
                        if (newSVal >= parseInt(inputLookbackStart.min) && newEVal <= parseInt(inputLookbackEnd.max)) {{
                            inputLookbackStart.value = newSVal;
                            inputLookbackEnd.value = newEVal;
                            
                            lblLookbackStart.innerText = lookbackDates[newSVal.toString()];
                            lblLookbackEnd.innerText = lookbackDates[newEVal.toString()] + (newEVal === 0 ? " (Today)" : "");
                            changed = true;
                        }}
                    }}

                    if (changed) {{
                        clearTimeout(renderTimer);
                        renderTimer = setTimeout(renderMap, 75);
                    }}
                }}
            }}
        }});

        inputLookbackStart.addEventListener('input', function() {{
            if (parseInt(this.value) > parseInt(inputLookbackEnd.value)) {{
                inputLookbackEnd.value = this.value;
            }}
            lblLookbackStart.innerText = lookbackDates[this.value];
            
            clearTimeout(renderTimer);
            renderTimer = setTimeout(renderMap, 75);
        }});
        
        inputLookbackEnd.addEventListener('input', function() {{
            if (parseInt(this.value) < parseInt(inputLookbackStart.value)) {{
                inputLookbackStart.value = this.value;
            }}
            lblLookbackEnd.innerText = lookbackDates[this.value] + (this.value === "0" ? " (Today)" : "");
            
            clearTimeout(renderTimer);
            renderTimer = setTimeout(renderMap, 75);
        }});

        inputCrest.addEventListener('input', function() {{
            lblCrest.innerText = "Day " + this.value;
            if (dayBounds[this.value]) lblCrestDates.innerText = dayBounds[this.value];
            
            clearTimeout(renderTimer);
            renderTimer = setTimeout(renderMap, 75);
        }});

        inputForecast.addEventListener('input', function() {{
            lblForecast.innerText = fxLabels[this.value.toString()];
            
            clearTimeout(renderTimer);
            renderTimer = setTimeout(renderMap, 75);
        }});
        
        map.on('overlayadd', function(e) {{
            if (e.name.indexOf("Record") !== -1) activeStatuses["Record"] = true;
            if (e.name.indexOf("Major") !== -1) activeStatuses["Major"] = true;
            if (e.name.indexOf("Moderate") !== -1) activeStatuses["Moderate"] = true;
            if (e.name.indexOf("Minor") !== -1) activeStatuses["Minor"] = true;
            if (e.name.indexOf("Action") !== -1) activeStatuses["Action"] = true;
            updateStats();
            if (activeMode !== 'base') renderMap();
        }});
        
        map.on('overlayremove', function(e) {{
            if (e.name.indexOf("Record") !== -1) activeStatuses["Record"] = false;
            if (e.name.indexOf("Major") !== -1) activeStatuses["Major"] = false;
            if (e.name.indexOf("Moderate") !== -1) activeStatuses["Moderate"] = false;
            if (e.name.indexOf("Minor") !== -1) activeStatuses["Minor"] = false;
            if (e.name.indexOf("Action") !== -1) activeStatuses["Action"] = false;
            updateStats();
            if (activeMode !== 'base') renderMap();
        }});
        
        function getTimelineIcon(color, isCrested) {{
            var svg;
            if (isCrested) {{
                svg = `<svg width="24" height="24" viewBox="0 0 24 24" style="display: block; margin: 0 auto;">
                           <polygon points="2,4 22,4 12,22" fill="${{color}}" stroke="black" stroke-width="1.5"/>
                       </svg>`;
            }} else {{
                svg = `<svg width="24" height="24" viewBox="0 0 24 24" style="display: block; margin: 0 auto;">
                           <polygon points="12,2 22,20 2,20" fill="${{color}}" stroke="black" stroke-width="1.5"/>
                       </svg>`;
            }}
            return L.divIcon({{
                html: `<div style="text-align: center; width: 24px;">${{svg}}</div>`,
                className: '',
                iconSize: [24, 24],
                iconAnchor: [12, 12]
            }});
        }}
        
        function updateStats() {{
            var bounds = map.getBounds();
            var obs_flood = 0; var fx_only = 0; var fx_record = 0; var fx_major = 0; var fx_moderate = 0; var fx_minor = 0; var fx_action = 0; var day_count = 0;
            var daysCount = {{1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0}};
            var activeDay = inputCrest.value;
            
            if (activeMode === 'lookback') {{
                var s_offset = Math.abs(parseInt(inputLookbackEnd.value));
                var e_offset = Math.abs(parseInt(inputLookbackStart.value));
                
                baseData.forEach(function(d) {{
                    if (bounds.contains(L.latLng(d.la, d.lo))) {{
                        let max_sev = 0;
                        let best_status = "Normal";
                        for(let j = s_offset; j <= e_offset; j++) {{
                            if(d.past_statuses[j] !== undefined) {{
                                let sev = severityMapDict[d.past_statuses[j]];
                                if(sev > max_sev) {{
                                    max_sev = sev;
                                    best_status = d.past_statuses[j];
                                }}
                            }}
                        }}
                        
                        if (best_status === "Record") fx_record++;
                        else if (best_status === "Major") fx_major++;
                        else if (best_status === "Moderate") fx_moderate++;
                        else if (best_status === "Minor") fx_minor++;
                        else if (best_status === "Action") fx_action++;
                    }}
                }});
            }} 
            else if (activeMode === 'forecast') {{
                var step = parseInt(inputForecast.value);
                var targetDt = new Date(timelineStartDt.getTime() + (step * 6 * 3600 * 1000));
                
                baseData.forEach(function(d) {{
                    if (bounds.contains(L.latLng(d.la, d.lo))) {{
                        var sliceVal = getStageAtTime(d.timeseries, targetDt);
                        if (sliceVal !== null) {{
                            var status = getFloodStatusJS(sliceVal, d.thresholds, d.record);
                            if (status === "Record") fx_record++;
                            else if (status === "Major") fx_major++;
                            else if (status === "Moderate") fx_moderate++;
                            else if (status === "Minor") fx_minor++;
                            else if (status === "Action") fx_action++;
                        }}
                    }}
                }});
            }}
            else {{
                baseData.forEach(function(d) {{
                    if (bounds.contains(L.latLng(d.la, d.lo))) {{
                        if (d.current_status !== "Normal") obs_flood++;
                        if (d.forecast_status !== "Normal" && d.current_status === "Normal") fx_only++;
                        
                        if (d.forecast_status === "Record") fx_record++;
                        else if (d.forecast_status === "Major") fx_major++;
                        else if (d.forecast_status === "Moderate") fx_moderate++;
                        else if (d.forecast_status === "Minor") fx_minor++;
                        else if (d.forecast_status === "Action") fx_action++;
                        
                        if (activeStatuses[d.display_status] && !d.is_crested) {{
                            var parsedDay = parseInt(d.day_str.replace('+', ''));
                            if (parsedDay >= 1 && parsedDay <= 7) {{
                                daysCount[parsedDay]++;
                            }}
                            if (parsedDay == activeDay) day_count++;
                        }}
                    }}
                }});
            }}
            
            document.getElementById('count-obs').innerText = (activeMode==='lookback') ? "N/A" : obs_flood;
            document.getElementById('count-fxonly').innerText = (activeMode==='forecast' || activeMode==='lookback') ? "N/A" : fx_only;
            document.getElementById('count-record').innerText = fx_record;
            document.getElementById('count-major').innerText = fx_major;
            document.getElementById('count-moderate').innerText = fx_moderate;
            document.getElementById('count-minor').innerText = fx_minor;
            document.getElementById('count-action').innerText = fx_action;
            
            if (activeMode === 'crest') {{
                document.getElementById('count-day-label').innerText = activeDay;
                document.getElementById('count-day').innerText = day_count;
            }}
            
            for (var i = 1; i <= 7; i++) {{
                document.getElementById('count-day' + i).innerText = (activeMode==='lookback') ? "0" : daysCount[i];
            }}
        }}

        map.on('moveend', updateStats);
        map.on('zoomend', updateStats);
        
        function renderMap() {{
            dynamicLayer.clearLayers();
            if (activeChartInstance) {{ activeChartInstance.destroy(); activeChartInstance = null; }}
            
            btnStatic.style.background = (activeMode === 'base') ? '#555' : '#222';
            btnLookback.style.background = (activeMode === 'lookback') ? '#555' : '#222';
            btnCrest.style.background = (activeMode === 'crest') ? '#555' : '#222';
            btnHeatmap.style.background = (activeMode === 'heatmap') ? '#555' : '#222';
            btnForecast.style.background = (activeMode === 'forecast') ? '#555' : '#222';
            
            if (activeMode === 'base') {{
                if (markerPane) markerPane.style.display = 'block';
                uiLookback.style.display = 'none';
                uiCrest.style.display = 'none';
                uiForecast.style.display = 'none';
                uiCrestCount.style.display = 'none';
                uiHeatLegend.style.display = 'none';
                
                document.querySelectorAll('.leaflet-marker-icon').forEach(function(el) {{
                    el.style.display = ''; 
                }});
            }} 
            else if (activeMode === 'lookback') {{
                if (markerPane) markerPane.style.display = 'none';
                uiLookback.style.display = 'block';
                uiCrest.style.display = 'none';
                uiForecast.style.display = 'none';
                uiCrestCount.style.display = 'none';
                uiHeatLegend.style.display = 'none';
                
                var startVal = inputLookbackStart.value;
                var endVal = inputLookbackEnd.value;
                
                var s_offset = Math.abs(parseInt(endVal));
                var e_offset = Math.abs(parseInt(startVal));
                
                baseData.forEach(function(d) {{
                    let max_sev = 0;
                    let max_val = -100.0;
                    let best_status = "Normal";
                    
                    for(let j = s_offset; j <= e_offset; j++) {{
                        if(d.past_stages[j] !== undefined && d.past_stages[j] > max_val) {{
                            max_val = d.past_stages[j];
                        }}
                        if(d.past_statuses[j] !== undefined) {{
                            let sev = severityMapDict[d.past_statuses[j]];
                            if(sev > max_sev) {{
                                max_sev = sev;
                                best_status = d.past_statuses[j];
                            }}
                        }}
                    }}
                    
                    if (best_status !== "Normal" && activeStatuses[best_status]) {{
                        var winColor = colorMap[best_status];
                        var dynamicZIndex = severityMapDict[best_status] * 1000;
                        
                        var circle = L.circleMarker([d.la, d.lo], {{
                            radius: 8, fillColor: winColor, color: "black", weight: 1.5, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        
                        if (circle.setZIndexOffset) circle.setZIndexOffset(dynamicZIndex);
                        circle.bindPopup(`<div class='dynamic-popup' data-lid='${{d.id}}' data-mode='lookback'>Loading...</div>`, {{maxWidth: 500}});
                        dynamicLayer.addLayer(circle);
                    }}
                }});
            }}
            else if (activeMode === 'crest') {{
                if (markerPane) markerPane.style.display = 'none';
                uiLookback.style.display = 'none';
                uiCrest.style.display = 'block';
                uiForecast.style.display = 'none';
                uiCrestCount.style.display = 'block';
                uiHeatLegend.style.display = 'none';
                
                var day = inputCrest.value;
                
                baseData.forEach(function(d) {{
                    var cleanDay = d.day_str.replace('+', '');
                    if (cleanDay == day && activeStatuses[d.display_status] && !d.is_crested) {{
                        var dynamicZIndex = severityMapDict[d.display_status] * 1000;
                        
                        var circle = L.circleMarker([d.la, d.lo], {{
                            radius: 8, fillColor: d.color, color: "black", weight: 1.5, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        
                        if (circle.setZIndexOffset) circle.setZIndexOffset(dynamicZIndex);
                        circle.bindPopup(`<div class='dynamic-popup' data-lid='${{d.id}}' data-mode='crest'>Loading...</div>`, {{maxWidth: 500}});
                        dynamicLayer.addLayer(circle);
                    }}
                }});
            }}
            else if (activeMode === 'heatmap') {{
                if (markerPane) markerPane.style.display = 'none';
                uiLookback.style.display = 'none';
                uiCrest.style.display = 'none';
                uiForecast.style.display = 'none';
                uiCrestCount.style.display = 'none';
                uiHeatLegend.style.display = 'block';
                
                baseData.forEach(function(d) {{
                    if (activeStatuses[d.display_status] && !d.is_crested && d.has_forecast) {{
                        var dynamicZIndex = severityMapDict[d.display_status] * 1000;
                        var assignedColor = heatColors[d.day_str] || "#FFC371";
                        
                        var circle = L.circleMarker([d.la, d.lo], {{
                            radius: 8, fillColor: assignedColor, color: "black", weight: 1.2, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        
                        if (circle.setZIndexOffset) circle.setZIndexOffset(dynamicZIndex);
                        circle.bindPopup(`<div class='dynamic-popup' data-lid='${{d.id}}' data-mode='heatmap'>Loading...</div>`, {{maxWidth: 500}});
                        dynamicLayer.addLayer(circle);
                    }}
                }});
            }}
            else if (activeMode === 'forecast') {{
                if (markerPane) markerPane.style.display = 'none';
                uiLookback.style.display = 'none';
                uiCrest.style.display = 'none';
                uiForecast.style.display = 'block';
                uiCrestCount.style.display = 'none';
                uiHeatLegend.style.display = 'none';
                
                var step = parseInt(inputForecast.value);
                var targetDt = new Date(timelineStartDt.getTime() + (step * 6 * 3600 * 1000));
                
                baseData.forEach(function(d) {{
                    var sliceVal = getStageAtTime(d.timeseries, targetDt);
                    if (sliceVal !== null) {{
                        var status = getFloodStatusJS(sliceVal, d.thresholds, d.record);
                        if (status !== "Normal" && activeStatuses[status]) {{
                            var prevDt = new Date(targetDt.getTime() - (6 * 3600 * 1000));
                            var prevVal = getStageAtTime(d.timeseries, prevDt);
                            var isCrested = (prevVal !== null && sliceVal < prevVal);
                            
                            var dynamicZIndex = severityMapDict[status] * 1000;
                            var marker = L.marker([d.la, d.lo], {{
                                icon: getTimelineIcon(colorMap[status], isCrested),
                                pane: 'sliderTimelinePane',
                                zIndexOffset: dynamicZIndex
                            }});
                            
                            marker.bindPopup(`<div class='dynamic-popup' data-lid='${{d.id}}' data-mode='forecast' data-step='${{step}}'>Loading...</div>`, {{maxWidth: 500}});
                            dynamicLayer.addLayer(marker);
                        }}
                    }}
                }});
            }}
            
            updateStats();
        }}
        
        btnStatic.onclick = function() {{ activeMode = 'base'; renderMap(); }};
        btnLookback.onclick = function() {{ activeMode = 'lookback'; renderMap(); }};
        btnCrest.onclick = function() {{ activeMode = 'crest'; renderMap(); }};
        btnHeatmap.onclick = function() {{ activeMode = 'heatmap'; renderMap(); }};
        btnForecast.onclick = function() {{ activeMode = 'forecast'; renderMap(); }};
        
        renderMap(); 
    }});
    </script>
    """
    
    m.get_root().html.add_child(folium.Element(custom_ui_html))
        
    m.save(DASHBOARD_FILE)
    print(f"Dashboard updated successfully: {DASHBOARD_FILE}")

if __name__ == "__main__":
    if "ipykernel" in sys.modules:
        import nest_asyncio
        nest_asyncio.apply()
        
    asyncio.run(generate_dashboard())