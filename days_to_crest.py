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
DASHBOARD_FILE = "days_to_crest.html"

MAX_CONCURRENT_REQUESTS = 100 
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
        return "No forecast available", -1.0, "", None

    max_stage = -1.0
    crest_time_str = None
    
    for entry in forecast_data:
        stage = entry.get("primary")
        if stage is not None and stage > max_stage:
            max_stage = stage
            crest_time_str = entry.get("validTime")
            
    if not crest_time_str:
        return "Unknown", max_stage, "", None
        
    try:
        crest_dt = datetime.fromisoformat(crest_time_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)
        
        time_formatted = crest_dt.strftime('%m-%d %H:%M UTC')
        crest_day_str = crest_dt.strftime('%a %p') 
        
        shifted_now = now - timedelta(hours=12)
        shifted_crest = crest_dt - timedelta(hours=12)
        delta_days = (shifted_crest.date() - shifted_now.date()).days
        day_num = str(max(1, delta_days + 1)) 
        
        crest_str = f"Forecast Crest: {max_stage}ft on {crest_day_str} ({time_formatted})"
        return crest_str, max_stage, day_num, crest_dt 
        
    except Exception:
        return f"Forecast Crest: {max_stage}ft", max_stage, "", None

def get_stage_at_time(forecast_data, target_dt):
    if not forecast_data: return None
    try:
        first_dt = datetime.fromisoformat(forecast_data[0]["validTime"].replace('Z', '+00:00'))
        last_dt = datetime.fromisoformat(forecast_data[-1]["validTime"].replace('Z', '+00:00'))
        
        if target_dt < first_dt:
            return forecast_data[0].get("primary")
            
        if target_dt > last_dt:
            return None
            
        for i in range(len(forecast_data) - 1):
            t1 = datetime.fromisoformat(forecast_data[i]["validTime"].replace('Z', '+00:00'))
            t2 = datetime.fromisoformat(forecast_data[i+1]["validTime"].replace('Z', '+00:00'))
            if t1 <= target_dt <= t2:
                s1 = forecast_data[i].get("primary")
                s2 = forecast_data[i+1].get("primary")
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
    timeout = aiohttp.ClientTimeout(total=600) 
    
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
    
    # --- FIX: Initialize with no tiles, then explicitly add 'Light Mode' ---
    m = folium.Map(location=[39.8, -98.5], zoom_start=4, tiles=None)
    folium.TileLayer('CartoDB positron', name='Light Mode').add_to(m)
    folium.TileLayer('CartoDB dark_matter', name='Dark Mode', opacity=0.85, show=False).add_to(m)
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
    for i in range(29):
        t = timeline_start_dt + timedelta(hours=6*i)
        forecast_timeline_labels[str(i)] = t.strftime('%b %d %H:%M UTC')

    for lid, meta in metadata.items():
        flood_categories = meta.get("flood", {}).get("categories", {})
        
        if not has_valid_thresholds(flood_categories):
            continue
            
        lat = meta.get("latitude")
        lon = meta.get("longitude")
        sf_data = stageflow_data.get(lid, {})
        
        observed_array = sf_data.get("observed", {}).get("data", [])
        
        current_stage = None
        max_recent_obs = -100.0
        
        if observed_array:
            for obs in reversed(observed_array):
                val = obs.get("primary")
                if val is not None and val > -50:
                    current_stage = val
                    break
                    
            for obs in observed_array:
                val = obs.get("primary")
                time_str = obs.get("validTime")
                if val is not None and val > -50 and time_str:
                    obs_dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    if (now_utc - obs_dt).total_seconds() <= 6 * 3600:
                        if val > max_recent_obs:
                            max_recent_obs = val
                            
        current_status = get_flood_status(current_stage, flood_categories)
        
        forecast_array = sf_data.get("forecast", {}).get("data", [])
        
        if not forecast_array:
            continue
            
        crest_str, forecast_max, crest_day_num, crest_dt = get_crest_info(forecast_array)
        forecast_status = get_flood_status(forecast_max, flood_categories)
        
        if current_status == "Normal" and forecast_status == "Normal":
            continue
            
        is_crested_overall = False
        if current_stage is not None and forecast_array and forecast_max is not None:
            first_fx_stage = forecast_array[0].get("primary")
            
            if current_stage >= forecast_max - 0.05:
                is_crested_overall = True
            elif first_fx_stage is not None and first_fx_stage >= forecast_max - 0.05 and current_stage >= first_fx_stage - 0.05:
                is_crested_overall = True
            elif max_recent_obs > forecast_max - 0.05 and current_stage <= max_recent_obs:
                if crest_dt and (crest_dt - now_utc).total_seconds() < 24 * 3600:
                    is_crested_overall = True
                    
        if is_crested_overall:
            peak_val = max(current_stage, max_recent_obs) if max_recent_obs > -50 else current_stage
            crest_str = f"Status: Crested & Receding (Recent Peak: {round(peak_val, 2)}ft)"
            forecast_status = get_flood_status(peak_val, flood_categories)
                
        nwps_url = f"https://water.noaa.gov/gauges/{lid}"
        img_url = f"https://water.noaa.gov/resources/hydrographs/{lid.lower()}_hg.png"
                
        if forecast_array:
            for i in range(29):
                target_slice = timeline_start_dt + timedelta(hours=6*i)
                stage_at_slice = get_stage_at_time(forecast_array, target_slice)
                
                if stage_at_slice is not None:
                    status_at_slice = get_flood_status(stage_at_slice, flood_categories)
                    
                    if status_at_slice != "Normal":
                        if is_crested_overall:
                            step_is_crested = True
                        elif crest_dt:
                            step_is_crested = (target_slice >= crest_dt)
                        else:
                            step_is_crested = False
                            
                        popup_html_slice = f"""
                        <div style="width: 450px; font-family: Arial, sans-serif; color: black;">
                            <h4 style="margin-bottom: 5px; margin-top: 0px;">Gauge: {lid}</h4>
                            <b>Interpolated Stage at {forecast_timeline_labels[str(i)]}:</b> {round(stage_at_slice, 2)}ft<br>
                            <hr style="margin: 5px 0;">
                            <b>Crest Info:</b><br>{crest_str}<br>
                            <hr style="margin: 5px 0;">
                            <img src="{img_url}" alt="NWPS Hydrograph" style="width: 100%; border: 1px solid #ccc; border-radius: 4px;">
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
        <div style="width: 450px; font-family: Arial, sans-serif; color: black;">
            <h4 style="margin-bottom: 5px; margin-top: 0px;">Gauge: {lid}</h4>
            <b>Current Stage:</b> {current_stage if current_stage else 'N/A'}ft<br>
            <hr style="margin: 5px 0;">
            <b>Crest Info:</b><br>{crest_str}<br>
            <hr style="margin: 5px 0;">
            <img src="{img_url}" alt="NWPS Hydrograph" style="width: 100%; border: 1px solid #ccc; border-radius: 4px;">
            <br>
            <a href="{nwps_url}" target="_blank" style="display: inline-block; margin-top: 8px; color: #0055ff; text-decoration: none; font-weight: bold;">&#128279; View Live NWPS Page</a>
        </div>
        """
        
        display_status = forecast_status if forecast_status != "Normal" else current_status
        
        if display_status == "Normal":
            continue
            
        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=500),
            icon=folium.DivIcon(html=icon_html, icon_size=(34, 34), icon_anchor=(17, 17))
        ).add_to(layer_map[display_status])
        
        base_slider_data.append({
            "lid": lid,
            "lat": lat,
            "lon": lon,
            "color": fx_color,
            "day": int(crest_day_num) if crest_day_num.isdigit() else 1,
            "current_status": current_status,
            "forecast_status": forecast_status,
            "display_status": display_status, 
            "is_crested": is_crested_overall, 
            "popup": popup_html
        })

    for fg in layer_map.values():
        fg.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    
    shifted_now = now_utc - timedelta(hours=12)
    base_date = shifted_now.date()
    day_bounds = {}
    for i in range(1, 8):
        start_dt = datetime(base_date.year, base_date.month, base_date.day, 12, 0, tzinfo=timezone.utc) + timedelta(days=i-1)
        end_dt = start_dt + timedelta(days=1)
        day_bounds[str(i)] = f"{start_dt.strftime('%b %d %H:%M')} UTC - {end_dt.strftime('%b %d %H:%M')} UTC"

    custom_ui_html = f"""
    <style>
        .leaflet-control-layers {{
            font-family: 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif !important;
            font-size: 16px !important; 
            line-height: 1.5 !important;
            padding: 12px 18px !important;
            border-radius: 8px !important;
            box-shadow: 2px 2px 10px rgba(0,0,0,0.3) !important;
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
            border-top: 1px solid #ccc !important;
        }}
        .leaflet-control-layers input[type="checkbox"],
        .leaflet-control-layers input[type="radio"] {{
            width: 20px;
            height: 20px;
            margin-right: 10px;
            cursor: pointer;
        }}
    </style>

    <div style="position: fixed; top: 15px; left: 50%; transform: translateX(-50%); z-index: 9999; display: flex; gap: 15px; background: rgba(30, 30, 30, 0.85); padding: 12px; border-radius: 8px; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5);">
        <div id="btnStaticMode" style="background: #555; color: white; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 15px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center;">
            &#128506; Static Map
        </div>
        <div id="btnCrestSlider" style="background: #222; color: white; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 15px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center;">
            &#9202; Crest Time-Slider
        </div>
        <div id="btnForecastSlider" style="background: #222; color: white; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-family: 'Segoe UI', Arial, sans-serif; font-weight: bold; font-size: 15px; box-shadow: 2px 2px 5px rgba(0,0,0,0.5); text-align: center;">
            &#128200; Forecast Time-Slider
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
    
    <div id="crestSliderUI" style="position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%); z-index: 9999; background: rgba(30, 30, 30, 0.95); padding: 15px 25px; border-radius: 8px; display: none; color: white; font-family: 'Segoe UI', Arial, sans-serif; text-align: center; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); width: 340px;">
        <h4 style="margin: 0 0 10px 0; font-size: 16px;">Forecast Crest Timeline</h4>
        <input type="range" id="crestInput" min="1" max="7" value="1" step="1" style="width: 100%; cursor: pointer;">
        <div id="crestLabel" style="margin-top: 10px; font-weight: bold; font-size: 18px;">Day 1</div>
        <div id="crestDateRange" style="font-size: 13px; color: #00ffcc; margin-top: 5px;"></div>
    </div>
    
    <div id="forecastSliderUI" style="position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%); z-index: 9999; background: rgba(30, 30, 30, 0.95); padding: 15px 25px; border-radius: 8px; display: none; color: white; font-family: 'Segoe UI', Arial, sans-serif; text-align: center; border: 1px solid #555; box-shadow: 2px 2px 10px rgba(0,0,0,0.5); width: 340px;">
        <h4 style="margin: 0 0 10px 0; font-size: 16px;">6-Hour Status Timeline</h4>
        <input type="range" id="forecastInput" min="0" max="28" value="0" step="1" style="width: 100%; cursor: pointer;">
        <div id="forecastLabel" style="margin-top: 10px; font-weight: bold; font-size: 18px;">Initializing...</div>
    </div>
    
    <script>
    window.addEventListener("load", function() {{
        var map = {m.get_name()}; 
        var baseData = {json.dumps(base_slider_data)};
        var forecastData = {json.dumps(forecast_timeline_data)};
        var dayBounds = {json.dumps(day_bounds)};
        var fxLabels = {json.dumps(forecast_timeline_labels)};
        
        map.createPane('sliderTimelinePane');
        map.getPane('sliderTimelinePane').style.zIndex = 650;
        
        var dynamicLayer = L.layerGroup().addTo(map);
        
        var btnStatic = document.getElementById('btnStaticMode');
        var btnCrest = document.getElementById('btnCrestSlider');
        var btnForecast = document.getElementById('btnForecastSlider');
        
        var uiCrest = document.getElementById('crestSliderUI');
        var uiForecast = document.getElementById('forecastSliderUI');
        var uiCrestCount = document.getElementById('dynamicCrestCount');
        
        var inputCrest = document.getElementById('crestInput');
        var inputForecast = document.getElementById('forecastInput');
        
        var lblCrest = document.getElementById('crestLabel');
        var lblCrestDates = document.getElementById('crestDateRange');
        var lblForecast = document.getElementById('forecastLabel');
        
        var markerPane = document.querySelector('.leaflet-marker-pane');
        var activeMode = 'base'; 
        
        var activeStatuses = {{ "Major": true, "Moderate": true, "Minor": true, "Action": false }};
        
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
            
            baseData.forEach(function(d) {{
                if (bounds.contains(L.latLng(d.lat, d.lon))) {{
                    if (d.current_status !== "Normal") obs_flood++;
                    if (d.forecast_status !== "Normal" && d.current_status === "Normal") fx_only++;
                    
                    if (d.forecast_status === "Major") fx_major++;
                    else if (d.forecast_status === "Moderate") fx_moderate++;
                    else if (d.forecast_status === "Minor") fx_minor++;
                    else if (d.forecast_status === "Action") fx_action++;
                    
                    if (activeStatuses[d.display_status] && !d.is_crested) {{
                        if (d.day >= 1 && d.day <= 7) {{
                            daysCount[d.day]++;
                        }}
                        if (d.day == activeDay) day_count++;
                    }}
                }}
            }});
            
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
            
            document.getElementById('count-obs').innerText = obs_flood;
            document.getElementById('count-fxonly').innerText = (activeMode==='forecast') ? "N/A" : fx_only;
            document.getElementById('count-major').innerText = fx_major;
            document.getElementById('count-moderate').innerText = fx_moderate;
            document.getElementById('count-minor').innerText = fx_minor;
            document.getElementById('count-action').innerText = fx_action;
            
            if (activeMode === 'crest') {{
                document.getElementById('count-day-label').innerText = activeDay;
                document.getElementById('count-day').innerText = day_count;
            }}
            
            for (var i = 1; i <= 7; i++) {{
                document.getElementById('count-day' + i).innerText = daysCount[i];
            }}
        }}

        map.on('moveend', updateStats);
        map.on('zoomend', updateStats);
        
        function renderMap() {{
            dynamicLayer.clearLayers();
            
            btnStatic.style.background = (activeMode === 'base') ? '#555' : '#222';
            btnCrest.style.background = (activeMode === 'crest') ? '#555' : '#222';
            btnForecast.style.background = (activeMode === 'forecast') ? '#555' : '#222';
            
            if (activeMode === 'base') {{
                if (markerPane) markerPane.style.display = 'block';
                uiCrest.style.display = 'none';
                uiForecast.style.display = 'none';
                uiCrestCount.style.display = 'none';
            }} 
            else if (activeMode === 'crest') {{
                if (markerPane) markerPane.style.display = 'none';
                uiCrest.style.display = 'block';
                uiForecast.style.display = 'none';
                uiCrestCount.style.display = 'block';
                
                var day = inputCrest.value;
                lblCrest.innerText = "Day " + day;
                if (dayBounds[day]) lblCrestDates.innerText = dayBounds[day];
                
                baseData.forEach(function(d) {{
                    if (d.day == day && activeStatuses[d.display_status] && !d.is_crested) {{
                        var circle = L.circleMarker([d.lat, d.lon], {{
                            radius: 8, fillColor: d.color, color: "black", weight: 1.5, opacity: 1, fillOpacity: 0.9, pane: 'sliderTimelinePane'
                        }});
                        circle.bindPopup(d.popup, {{maxWidth: 500}});
                        dynamicLayer.addLayer(circle);
                    }}
                }});
            }}
            else if (activeMode === 'forecast') {{
                if (markerPane) markerPane.style.display = 'none';
                uiCrest.style.display = 'none';
                uiForecast.style.display = 'block';
                uiCrestCount.style.display = 'none';
                
                var step = inputForecast.value;
                lblForecast.innerText = fxLabels[step];
                
                forecastData.forEach(function(d) {{
                    if (d.step == step && activeStatuses[d.status]) {{
                        var marker = L.marker([d.lat, d.lon], {{
                            icon: getTimelineIcon(d.color, d.crested),
                            pane: 'sliderTimelinePane'
                        }});
                        marker.bindPopup(d.popup, {{maxWidth: 500}});
                        dynamicLayer.addLayer(marker);
                    }}
                }});
            }}
            
            updateStats();
        }}
        
        btnStatic.onclick = function() {{ activeMode = 'base'; renderMap(); }};
        btnCrest.onclick = function() {{ activeMode = 'crest'; renderMap(); }};
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