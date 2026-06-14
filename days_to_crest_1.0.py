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

MAX_CONCURRENT_REQUESTS = 25
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

def get_flood_status(stage, categories):
    if stage is None or not categories:
        return "Normal"
    for status_name in ["major", "moderate", "minor", "action"]:
        category_data = categories.get(status_name)
        if category_data is not None:
            threshold = category_data.get("stage") if isinstance(category_data, dict) else category_data
            if threshold is not None and threshold > -50 and stage >= threshold:
                return status_name.capitalize()
    return "Normal"

def get_status_color(status):
    colors = {
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
async def fetch_all_gauge_lids(session):
    print("Discovering all NWPS gauges...")
    async with session.get(BASE_API_URL) as response:
        if response.status == 200:
            data = await response.json()
            if "gauges" in data:
                return [g.get("lid") for g in data["gauges"] if "lid" in g]
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
    results = await asyncio.gather(*tasks)
    
    for lid, data in results:
        if data and "latitude" in data and "longitude" in data:
            metadata[lid] = data
                
    with open(CACHE_FILE, "w") as f:
        json.dump(metadata, f)
        
    return metadata

async def generate_dashboard():
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=0, sock_connect=30, sock_read=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        metadata = await build_metadata_cache(session)
        
        print(f"Fetching real-time stage/flow updates for {len(metadata)} gauges...")
        stageflow_data = {}
        tasks = [fetch_stageflow(session, lid) for lid in metadata.keys()]
        results = await asyncio.gather(*tasks)
        
        for lid, data in results:
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

    fg_major = folium.FeatureGroup(name=f"<span style='color: #cc33ff; font-weight: bold; {txt_outline}'>&#9650; Major Flood</span>")
    fg_moderate = folium.FeatureGroup(name=f"<span style='color: #ff0000; font-weight: bold; {txt_outline}'>&#9650; Moderate Flood</span>")
    fg_minor = folium.FeatureGroup(name=f"<span style='color: #ff9900; font-weight: bold; {txt_outline}'>&#9650; Minor Flood</span>")
    fg_action = folium.FeatureGroup(name=f"<span style='color: #ffff00; font-weight: bold; {txt_outline}'>&#9650; Action Stage</span>", show=False)
    
    layer_map = {
        "Major": fg_major,
        "Moderate": fg_moderate,
        "Minor": fg_minor,
        "Action": fg_action
    }

    base_slider_data = [] 
    forecast_timeline_data = []

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

    severityMapDict = {"Major": 4, "Moderate": 3, "Minor": 2, "Action": 1, "Normal": 0}

    for lid, meta in metadata.items():
        if lid in ["ESLN8", "DCBN8"]:
            continue
            
        flood_categories = meta.get("flood", {}).get("categories", {})
        if not has_valid_thresholds(flood_categories):
            continue
            
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
                                    past_stages[days_ago_offset] = val
                                    past_statuses[days_ago_offset] = get_flood_status(val, flood_categories)
                    except Exception:
                        pass
                            
        current_status = get_flood_status(current_stage, flood_categories)
        
        forecast_max = -1.0
        max_idx = -1
        for idx, entry in enumerate(forecast_array):
            stage = entry.get("primary")
            # --- FIX: Replaced invalid && with Python 'and' keyword ---
            if stage is not None and stage > forecast_max:
                forecast_max = stage
                max_idx = idx

        first_fx_stage = forecast_array[0].get("primary") if forecast_array else None
        
        is_crested_overall = False
        if first_fx_stage is not None and first_fx_stage >= forecast_max - 0.05:
            is_crested_overall = True

        if is_crested_overall and first_fx_stage is not None:
            forecast_status = get_flood_status(first_fx_stage, flood_categories)
            crest_str = f"Status: Crested & Receding (Peak: {round(first_fx_stage, 2)}ft)"
            crest_day_num = "1"
        else:
            forecast_status = get_flood_status(forecast_max, flood_categories)
            crest_str, _, crest_day_num, _, _ = get_crest_info(forecast_array)
        
        max_lookback_status = "Normal"
        for offset_val in past_statuses.values():
            if offset_val == "Major": max_lookback_status = "Major"
            elif offset_val == "Moderate" and max_lookback_status not in ["Major"]: max_lookback_status = "Moderate"
            elif offset_val == "Minor" and max_lookback_status not in ["Major", "Moderate"]: max_lookback_status = "Minor"
            elif offset_val == "Action" and max_lookback_status == "Normal": max_lookback_status = "Action"
            
        if current_status == "Normal" and forecast_status == "Normal" and max_lookback_status == "Normal":
            continue
                
        nwps_url = f"https://water.noaa.gov/gauges/{lid}"
        img_url = f"https://water.noaa.gov/resources/hydrographs/{lid.lower()}_hg.png"
        
        current_stage_text = f"{round(current_stage, 2)}ft" if current_stage is not None else "N/A (No recent obs)"
                
        frontend_ts = [{"t": x["validTime"], "v": x["primary"]} for x in observed_array]
        
        t_levels = {}
        for s_lbl in ["major", "moderate", "minor", "action"]:
            c_data = flood_categories.get(s_lbl)
            val = c_data.get("stage") if isinstance(c_data, dict) else c_data
            if val is not None and val > -50:
                t_levels[s_lbl] = val

        if combined_timeseries:
            for i in range(-28, 29):
                target_slice = timeline_start_dt + timedelta(hours=6*i)
                stage_at_slice = get_stage_at_time(combined_timeseries, target_slice)
                
                if stage_at_slice is not None:
                    status_at_slice = get_flood_status(stage_at_slice, flood_categories)
                    
                    if status_at_slice != "Normal":
                        stage_prev = get_stage_at_time(combined_timeseries, target_slice - timedelta(hours=6))
                        step_is_crested = True if (stage_prev is not None and stage_at_slice < stage_prev) else False
                        time_label = "Past Stage" if i < 0 else "Forecast Stage"

                        popup_html_slice = f"""
                        <div style="width: 450px; font-family: Arial, sans-serif; color: black;" data-lid="{lid}" data-mode="timeline" data-step="{i}">
                            <h4 style="margin-bottom: 5px; margin-top: 0px;">Gauge: {lid}</h4>
                            <div style="font-size: 14px; background: #eef2f5; padding: 6px; border-radius: 4px; margin-bottom: 5px; border: 1px solid #ccc;">
                                <b>{crest_str}</b>
                            </div>
                            <b>Interpolated {time_label} at {forecast_timeline_labels[str(i)]}:</b> {round(stage_at_slice, 2)}ft<br>
                            <hr style="margin: 5px 0;">
                            <div class="popupChartContainer">
                                <canvas class="gaugeChartCanvas" style="width:100%; height:200px; display:none;"></canvas>
                                <img class="noaaStaticImg" src="{img_url}" alt="NWPS Hydrograph" style="width:100%; border:1px solid #ccc; border-radius:4px; display:none;">
                            </div>
                            <br>
                            <a href="{nwps_url}" target="_blank" style="display: inline-block; margin-top: 8px; color: #0055ff; text-decoration: none; font-weight: bold;">&#128279; View Live NWPS Page</a>
                        </div>
                        """
                            
                        forecast_timeline_data.append({
                            "step": i,
                            "lat": lat,
                            "lon": lon,
                            "color": get_status_color(status_at_slice),
                            "status": status_at_slice,
                            "crested": step_is_crested,
                            "popup": popup_html_slice
                        })
            
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
        
        popup_html = f"""
        <div style="width: 450px; font-family: Arial, sans-serif; color: black;" data-lid="{lid}" data-mode="standard">
            <h4 style="margin-bottom: 5px; margin-top: 0px;">Gauge: {lid}</h4>
            <div style="font-size: 14px; background: #eef2f5; padding: 6px; border-radius: 4px; margin-bottom: 5px; border: 1px solid #ccc;">
                <b>{crest_str}</b>
            </div>
            <b>Current Stage:</b> {current_stage_text}<br>
            <hr style="margin: 5px 0;">
            <div class="popupChartContainer">
                <canvas class="gaugeChartCanvas" style="width:100%; height:200px; display:none;"></canvas>
                <img class="noaaStaticImg" src="{img_url}" alt="NWPS Hydrograph" style="width:100%; border:1px solid #ccc; border-radius:4px;">
            </div>
            <br>
            <a href="{nwps_url}" target="_blank" style="display: inline-block; margin-top: 8px; color: #0055ff; text-decoration: none; font-weight: bold;">&#128279; View Live NWPS Page</a>
        </div>
        """
        
        display_status = forecast_status if forecast_status != "Normal" else current_status
        
        show_on_static = True
        if display_status == "Normal":
            display_status = max_lookback_status
            show_on_static = False
            
        if show_on_static:
            static_z_offset = severityMapDict.get(display_status, 0) * 1000
            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(popup_html, max_width=500),
                icon=folium.DivIcon(html=icon_html, icon_size=(34, 34), icon_anchor=(17, 17)),
                z_index_offset=static_z_offset
            ).add_to(layer_map[display_status])
        
        base_slider_data.append({
            "lid": lid,
            "lat": lat,
            "lon": lon,
            "color": fx_color,
            "past_stages": past_stages,
            "past_statuses": past_statuses,
            "max_obs_30d": max_obs_30d,
            "day_str": str(crest_day_num), 
            "has_forecast": len(forecast_array) > 0, 
            "current_status": current_status,
            "forecast_status": forecast_status,
            "display_status": display_status, 
            "is_crested": is_crested_overall, 
            "show_on_static": show_on_static,
            "thresholds": t_levels,
            "timeseries": frontend_ts,
            "popup": popup_html
        })

    fg_action.add_to(m)
    fg_minor.add_to(m)
    fg_moderate.add_to(m)
    fg_major.add_to(m)

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
        Major: <span id="count-major" style="font-weight: bold; color: #cc33ff;">0</span> | 
        Moderate: <span id="count-moderate" style="font-weight: bold; color: #ff0000;">0</span><br>
        Minor: <span id="count-minor" style="font-weight: bold; color: #ff9900;">0</span> | 
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
        var baseData = {json.dumps(base_slider_data)};
        var forecastData = {json.dumps(forecast_timeline_data)};
        var dayBounds = {json.dumps(day_bounds)};
        var fxLabels = {json.dumps(forecast_timeline_labels)};
        var lookbackDates = {json.dumps(lookback_dates)};
        
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
        
        var activeStatuses = {{ "Major": true, "Moderate": true, "Minor": true, "Action": false }};
        // --- FIX: Defined severityMapDict client-side for dynamic lookups ---
        var severityMapDict = {{ "Major": 4, "Moderate": 3, "Minor": 2, "Action": 1, "Normal": 0 }};
        var colorMap = {{ "Major": "#cc33ff", "Moderate": "#ff0000", "Minor": "#ff9900", "Action": "#ffff00", "Normal": "#00ff00" }};
        
        var activeChartInstance = null;

        map.on('popupopen', function(e) {{
            if (activeChartInstance) {{ activeChartInstance.destroy(); activeChartInstance = null; }}
            
            var container = e.popup._contentNode.querySelector('[data-lid]');
            if (!container) return;
            
            var lid = container.getAttribute('data-lid');
            var mode = container.getAttribute('data-mode');
            var canvas = container.querySelector('.gaugeChartCanvas');
            var staticImg = container.querySelector('.noaaStaticImg');
            
            var gData = baseData.find(d => d.lid === lid);
            if (!gData) return;

            var filteredData = [];
            var drawChart = true;

            if (mode === 'standard') {{
                drawChart = false;
                if (canvas) canvas.style.display = 'none';
                if (staticImg) staticImg.style.display = 'block';
            }} 
            else if (mode === 'lookback') {{
                if (staticImg) staticImg.style.display = 'none';
                if (canvas) canvas.style.display = 'block';
                
                let startLimit = new Date(); startLimit.setDate(startLimit.getDate() + parseInt(inputLookbackStart.value)); startLimit.setHours(0,0,0,0);
                let endLimit = new Date(); endLimit.setDate(endLimit.getDate() + parseInt(inputLookbackEnd.value)); endLimit.setHours(23,59,59,999);
                
                filteredData = gData.timeseries.filter(pt => {{
                    let d = new Date(pt.t);
                    return d >= startLimit && d <= endLimit;
                }});
            }} 
            else if (mode === 'timeline') {{
                var stepVal = parseInt(container.getAttribute('data-step'));
                
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
            
            var colorsEnum = {{ "major": "#cc33ff", "moderate": "#ff0000", "minor": "#ff9900", "action": "#ffff00" }};
            
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

        inputLookbackStart.addEventListener('input', function() {{
            if (parseInt(this.value) > parseInt(inputLookbackEnd.value)) {{
                inputLookbackEnd.value = this.value;
            }}
            renderMap();
        }});
        inputLookbackEnd.addEventListener('input', function() {{
            if (parseInt(this.value) < parseInt(inputLookbackStart.value)) {{
                inputLookbackStart.value = this.value;
            }}
            renderMap();
        }});
        
        map.on('overlayadd', function(e) {{
            if (e.name.indexOf("Major") !== -1) activeStatuses["Major"] = true;
            if (e.name.indexOf("Moderate") !== -1) activeStatuses["Moderate"] = true;
            if (e.name.indexOf("Minor") !== -1) activeStatuses["Minor"] = true;
            if (e.name.indexOf("Action") !== -1) activeStatuses["Action"] = true;
            updateStats();
            if (activeMode !== 'base') renderMap();
        }});
        
        map.on('overlayremove', function(e) {{
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
            var obs_flood = 0; var fx_only = 0; var fx_major = 0; var fx_moderate = 0; var fx_minor = 0; var fx_action = 0; var day_count = 0;
            var daysCount = {{1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0}};
            var activeDay = inputCrest.value;
            
            if (activeMode === 'lookback') {{
                var s_offset = Math.abs(parseInt(inputLookbackEnd.value));
                var e_offset = Math.abs(parseInt(inputLookbackStart.value));
                
                baseData.forEach(function(d) {{
                    if (bounds.contains(L.latLng(d.lat, d.lon))) {{
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
                        
                        if (best_status === "Major") fx_major++;
                        else if (best_status === "Moderate") fx_moderate++;
                        else if (best_status === "Minor") fx_minor++;
                        else if (best_status === "Action") fx_action++;
                    }}
                }});
            }} 
            else {{
                baseData.forEach(function(d) {{
                    if (bounds.contains(L.latLng(d.lat, d.lon))) {{
                        if (d.current_status !== "Normal") obs_flood++;
                        if (d.forecast_status !== "Normal" && d.current_status === "Normal") fx_only++;
                        
                        if (d.forecast_status === "Major") fx_major++;
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
            
            if (activeMode === 'forecast') {{
                var step = inputForecast.value;
                fx_major = 0; fx_moderate = 0; fx_minor = 0; fx_action = 0; 
                
                forecastData.forEach(function(d) {{
                    if (d.step == step && bounds.contains(L.latLng(d.lat, d.lon))) {{
                        if (d.status === "Major") fx_major++;
                        else if (d.status === "Moderate") fx_moderate++;
                        else if (d.status === "Minor") fx_minor++;
                        else if (d.status === "Action") fx_action++;
                    }}
                }});
            }}
            
            document.getElementById('count-obs').innerText = (activeMode==='lookback') ? "N/A" : obs_flood;
            document.getElementById('count-fxonly').innerText = (activeMode==='forecast' || activeMode==='lookback') ? "N/A" : fx_only;
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
                
                baseData.forEach(function(d) {{
                    let staticPopup = d.popup.replace('', `<b>Past 30-Day Max:</b> ${{d.max_obs_30d > -50 ? d.max_obs_30d.toFixed(2) + 'ft' : 'N/A'}}<br>`);
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
                
                lblLookbackStart.innerText = lookbackDates[startVal];
                lblLookbackEnd.innerText = lookbackDates[endVal] + (endVal === "0" ? " (Today)" : "");
                
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
                        
                        var circle = L.circleMarker([d.lat, d.lon], {{
                            radius: 8, fillColor: winColor, color: "black", weight: 1.5, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        
                        if (circle.setZIndexOffset) circle.setZIndexOffset(dynamicZIndex);
                        
                        let lbPopup = d.popup.replace('data-mode="standard"', 'data-mode="lookback"')
                                             .replace('', `<div style="font-size: 14px; color: #b30000; margin-bottom: 5px;"><b>Selected Window Max (${{lookbackDates[startVal]}} to ${{lookbackDates[endVal]}}):</b> ${{max_val > -50 ? max_val.toFixed(2) + 'ft' : 'N/A'}}</div>`);
                        
                        circle.bindPopup(lbPopup, {{maxWidth: 500}});
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
                lblCrest.innerText = "Day " + day;
                if (dayBounds[day]) lblCrestDates.innerText = dayBounds[day];
                
                baseData.forEach(function(d) {{
                    var cleanDay = d.day_str.replace('+', '');
                    if (cleanDay == day && activeStatuses[d.display_status] && !d.is_crested) {{
                        var dynamicZIndex = severityMapDict[d.display_status] * 1000;
                        
                        var circle = L.circleMarker([d.lat, d.lon], {{
                            radius: 8, fillColor: d.color, color: "black", weight: 1.5, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        
                        if (circle.setZIndexOffset) circle.setZIndexOffset(dynamicZIndex);
                        
                        let cPopup = d.popup.replace('', `<b>Past 30-Day Max:</b> ${{d.max_obs_30d > -50 ? d.max_obs_30d.toFixed(2) + 'ft' : 'N/A'}}<br>`);
                        circle.bindPopup(cPopup, {{maxWidth: 500}});
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
                        
                        var circle = L.circleMarker([d.lat, d.lon], {{
                            radius: 8, fillColor: assignedColor, color: "black", weight: 1.2, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        
                        if (circle.setZIndexOffset) circle.setZIndexOffset(dynamicZIndex);
                        
                        let hPopup = d.popup.replace('', `<b>Past 30-Day Max:</b> ${{d.max_obs_30d > -50 ? d.max_obs_30d.toFixed(2) + 'ft' : 'N/A'}}<br>`);
                        circle.bindPopup(hPopup, {{maxWidth: 500}});
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
                
                var step = inputForecast.value;
                lblForecast.innerText = fxLabels[step];
                
                forecastData.forEach(function(d) {{
                    if (d.step == step && activeStatuses[d.status]) {{
                        var dynamicZIndex = severityMapDict[d.status] * 1000;
                        
                        var marker = L.marker([d.lat, d.lon], {{
                            icon: getTimelineIcon(d.color, d.crested),
                            pane: 'sliderTimelinePane',
                            zIndexOffset: dynamicZIndex
                        }});
                        marker.bindPopup(d.popup, {{maxWidth: 500}});
                        dynamicLayer.addLayer(marker);
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
        
        inputCrest.oninput = renderMap;
        inputForecast.oninput = renderMap;
        
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