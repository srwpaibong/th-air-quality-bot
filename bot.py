import os
import requests
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import pandas as pd
import time

# ===== CONFIGURATION (รับค่าจาก GitHub Secrets) =====
AIR4THAI_KEY = os.getenv('AIR4THAI_KEY')
GISTDA_API_KEY = os.getenv('GISTDA_API_KEY')
TMD_3HR_KEY = os.getenv('TMD_3HR_KEY')
TMD_DAILY_KEY = os.getenv('TMD_DAILY_KEY') # Secret สำหรับพยากรณ์รายวัน
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',')

# เกณฑ์การตรวจสอบ (Thresholds)
STALE_THRESHOLD_MIN = 80
SPIKE_LIMIT = 50
MISSING_LIMIT_HRS = 5
FLATLINE_LIMIT_HRS = 4

# Headers เพื่อป้องกันการโดนบล็อก
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# ภูมิภาคและผู้รับผิดชอบ
REGION_CONFIG = {
    'ภาคเหนือ': {'prov': ['เชียงราย', 'เชียงใหม่', 'พะเยา', 'แพร่', 'น่าน', 'อุตรดิตถ์', 'ลำปาง', 'ตาก', 'ลำพูน', 'แม่ฮ่องสอน', 'สุโขทัย', 'กำแพงเพชร', 'เพชรบูรณ์', 'พิษณุโลก', 'นครสวรรค์', 'อุทัยธานี'], 'staff': 'พี่ป๊อปปี้'},
    'ภาคกลาง': {'prov': ['กาญจนบุรี', 'สุพรรณบุรี', 'อ่างทอง', 'ชัยนาท', 'สิงห์บุรี', 'ราชบุรี', 'ระยอง', 'สระบุรี', 'พระนครศรีอยุธยา', 'ลพบุรี', 'เพชรบุรี', 'สมุทรสงคราม', 'ประจวบคีรีขันธ์'], 'staff': 'พี่ป๊อปปี้'},
    'กรุงเทพฯและปริมณฑล': {'prov': ['กรุงเทพมหานคร', 'สมุทรสาคร', 'นนทบุรี', 'สมุทรปราการ', 'ปทุมธานี', 'นครปฐม'], 'staff': 'พี่ป๊อปปี้'},
    'ภาคใต้': {'prov': ['ชุมพร', 'ระนอง', 'พังงา', 'ภูเก็ต', 'สุราษฎร์ธานี', 'นครศรีธรรมราช', 'กระบี่', 'ตรัง', 'พัทลุง', 'สตูล', 'สงขลา', 'ปัตตานี', 'ยะลา', 'นราธิวาส'], 'staff': 'พี่หน่อย'},
    'ภาคตะวันออกเฉียงเหนือ': {'prov': ['ขอนแก่น', 'กาฬสินธุ์', 'ชัยภูมิ', 'นครพนม', 'นครราชสีมา', 'บึงกาฬ', 'บุรีรัมย์', 'มหาสารคาม', 'มุกดาหาร', 'ยโสธร', 'ร้อยเอ็ด', 'ศรีสะเกษ', 'สกลนคร', 'สุรินทร์', 'หนองคาย', 'หนองบัวลำภู', 'อำนาจเจริญ', 'อุดรธานี', 'อุบลราชธานี', 'เลย'], 'staff': 'พี่หน่อย'},
    'ภาคตะวันออก': {'prov': ['นครนายก', 'ฉะเชิงเทรา', 'ปราจีนบุรี', 'สระแก้ว', 'ชลบุรี', 'ระยอง', 'จันทบุรี', 'ตราด'], 'staff': 'พี่ฟรังก์'}
}

def get_now_th():
    return datetime.now(pytz.timezone('Asia/Bangkok'))

def format_duration(diff):
    days = diff.days
    hours = diff.seconds // 3600
    if days > 0: return f"{days} วัน {hours} ชม."
    return f"{hours} ชม."

def send_tg(text):
    for cid in TELEGRAM_CHAT_IDS:
        if not cid.strip(): continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": cid.strip(), "text": text, "parse_mode": "Markdown"}, timeout=15)
        except Exception as e: print(f"Error sending to {cid}: {e}")

def check_qa_issues_48h(station_id):
    """ตรวจสอบ QA ย้อนหลัง 48 ชม. (Spike, Missing, Flatline, Negative)"""
    try:
        now = get_now_th()
        edate = now.strftime('%Y-%m-%d')
        sdate = (now - timedelta(days=2)).strftime('%Y-%m-%d')
        url = f"http://air4thai.com/forweb/getHistoryData.php?stationID={station_id}&param=PM25&type=hr&sdate={sdate}&edate={edate}&stime=00&etime=23"
        res = requests.get(url, headers=HEADERS, timeout=15)
        raw_data = res.json()
        
        stations = raw_data.get('stations', [])
        if not stations: return None
        data_list = stations[0].get('data', [])
        if len(data_list) < 5: return None
        
        df = pd.DataFrame(data_list)
        df['PM25'] = pd.to_numeric(df['PM25'], errors='coerce')
        issues = []
        
        # 1. Spike Check (> 50 จากชม.ก่อน)
        if any(df['PM25'].diff() > SPIKE_LIMIT): issues.append(f"Spike 📈")
        # 2. Missing Data (> 5 ชม. ต่อเนื่อง)
        recent = df['PM25'].tail(24).tolist()
        miss, max_miss = 0, 0
        for v in recent:
            if pd.isna(v) or v == -1: miss += 1; max_miss = max(max_miss, miss)
            else: miss = 0
        if max_miss >= MISSING_LIMIT_HRS: issues.append(f"Missing {max_miss}h ❓")
        # 3. Flatline (> 4 ชม. ค่านิ่ง)
        if any(df['PM25'].rolling(window=FLATLINE_LIMIT_HRS).std() == 0): issues.append(f"Flatline 📏")
        # 4. Negative Value (ไม่ใช่ -1)
        if any((df['PM25'] < 0) & (df['PM25'] != -1)): issues.append("Negative ⚙️")
        
        return ", ".join(issues) if issues else None
    except: return None

def fetch_xml_with_retry(url, label):
    """ฟังก์ชันดึง XML พร้อมทำความสะอาดข้อมูลเบื้องต้นเพื่อลด Syntax Error"""
    for attempt in range(2):
        try:
            res = requests.get(url, headers=HEADERS, timeout=45)
            if res.status_code == 200:
                # ล้าง whitespace หัวท้ายและล้างอักขระ BOM เพื่อป้องกัน syntax error
                content = res.content.decode('utf-8-sig').strip()
                if content:
                    return ET.fromstring(content)
        except Exception as e:
            print(f"Fetch {label} failed: {e}")
            time.sleep(5)
    return None

def main():
    now = get_now_th()
    
    # --- 1. Fetch Data ---
    hourly_raw = []
    try:
        res = requests.get(f"http://air4thai.com/services/getAQI_County.php?key={AIR4THAI_KEY}", headers=HEADERS, timeout=30)
        hourly_raw = res.json()
    except Exception as e:
        print(f"Fetch Air4Thai Hourly failed: {e}")

    daily_raw = {"stations": []}
    try:
        res = requests.get("http://air4thai.com/forweb/getAQI_JSON.php", headers=HEADERS, timeout=30)
        daily_raw = res.json()
    except Exception as e:
        print(f"Fetch Air4Thai Daily failed: {e}")
    
    gistda_url = "https://api-gateway.gistda.or.th/api/2.0/resources/features/viirs/1day?limit=1000&offset=0&ct_tn=%E0%B8%A3%E0%B8%B2%E0%B8%8A%E0%B8%AD%E0%B8%B2%E0%B8%93%E0%B8%B2%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3%E0%B9%84%E0%B8%97%E0%B8%A2"
    hotspots_raw = {"features": []}
    try:
        res = requests.get(gistda_url, headers={**HEADERS, 'API-Key': GISTDA_API_KEY}, timeout=30)
        hotspots_raw = res.json()
    except Exception as e:
        print(f"Fetch GISTDA failed: {e}")
    
    # ดึงพยากรณ์รายวัน (Small XML)
    daily_weather_xml = fetch_xml_with_retry(f"https://data.tmd.go.th/api/DailyForecast/v2/?uid=api&ukey={TMD_DAILY_KEY}", "Daily Forecast")
    # ดึงสภาพอากาศราย 3 ชม. (Large XML - สำหรับวิเคราะห์ลม)
    weather_3hr_xml = fetch_xml_with_retry(f"https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey={TMD_3HR_KEY}", "3Hr Weather")

    # --- 2. Processing ---
    # Air Quality Validations
    if isinstance(hourly_raw, list):
        # ✅ แก้ไข: เพิ่มเงื่อนไข s.get('hourly_data') is not None เพื่อป้องกัน NoneType Error
        valid_h = [s for s in hourly_raw if s and isinstance(s, dict) and s.get('hourly_data') is not None]
    else:
        valid_h = []

    v1h = []
    for s in valid_h:
        val = s.get('hourly_data', {}).get('PM25')
        if val is not None:
            try:
                f_val = float(val)
                if f_val >= 0:
                    v1h.append(f_val)
            except: pass

    daily_stations = daily_raw.get('stations', [])
    v24h = []
    if isinstance(daily_stations, list):
        for s in daily_stations:
            val = s.get('AQILast', {}).get('PM25', {}).get('value')
            if val is not None:
                try:
                    f_val = float(val)
                    if f_val >= 0:
                        v24h.append(f_val)
                except: pass

    outdated_list, qa_list = [], []
    for s in valid_h:
        st_id, st_name, area = s.get('StationID'), s.get('StationNameTh'), s.get('AreaNameTh')
        if s.get('last_datetime'):
            try:
                diff = now - datetime.strptime(s['last_datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Bangkok'))
                if diff.total_seconds() > STALE_THRESHOLD_MIN * 60: outdated_list.append({'id': st_id, 'name': st_name, 'area': area, 'diff': diff})
            except: pass
        
        # QA Check
        cur_v = float(s['hourly_data'].get('PM25', -1))
        if cur_v > 150 or cur_v < -1 or st_id in ["05t", "12t"]:
            issue = check_qa_issues_48h(st_id)
            if issue: qa_list.append(f"• {st_id} | {st_name}: {issue}")

    # Weather Analysis (Wind/Rain)
    rain_provs, wind_data = [], {}
    if weather_3hr_xml is not None:
        for st in weather_3hr_xml.findall('.//Station'):
            p_node = st.find('Province')
            p = p_node.text.strip() if p_node is not None else "N/A"
            obs = st.find('Observation')
            if obs is not None:
                r_node = obs.find('Rainfall')
                if r_node is not None and r_node.text and float(r_node.text) > 0: rain_provs.append(p)
                w_node = obs.find('WindSpeed')
                if w_node is not None and w_node.text: wind_data[p] = float(w_node.text)

    # Weather Description (Daily Forecast)
    overall_desc = "ไม่พบข้อมูลพยากรณ์อากาศ"
    if daily_weather_xml is not None:
        overall_node = daily_weather_xml.find('.//OverallDescriptionThai')
        if overall_node is not None: overall_desc = overall_node.text.strip()

    # Hotspots
    features = hotspots_raw.get('features', [])
    h_provs = {}
    for f in features:
        props = f.get('properties', {})
        p = props.get('pv_tn', 'N/A')
        h_provs[p] = h_provs.get(p, 0) + 1
    top5_h = sorted(h_provs.items(), key=lambda x: x[1], reverse=True)[:5]

    # --- 3. Reporting ---
    # Msg 1: Overview
    msg1 = f"🌏 *สรุปสถานการณ์คุณภาพอากาศประเทศไทย*\n🕒 {now.strftime('%d/%m/%Y %H:%M')}\n\n"
    msg1 += f"📊 PM2.5 (1h): `{min(v1h) if v1h else 0}-{max(v1h) if v1h else 0}` | (24h): `{min(v24h) if v24h else 0}-{max(v24h) if v24h else 0}`\n"
    msg1 += f"⚠️ ไม่อัปเดต: `{len(outdated_list)}` | ผิดปกติ (QA): `{len(qa_list)}` สถานี\n\n"
    msg1 += f"🌤 *สภาวะอากาศ:* {overall_desc[:250]}...\n\n" # ตัดสั้นถ้าบรรยายยาวไป
    risk_areas = [p for p, w in wind_data.items() if w < 5 and p in h_provs]
    msg1 += f"🔍 *บทวิเคราะห์:*\n📍 เฝ้าระวัง (ลมนิ่ง+ไฟ): `{', '.join(list(set(risk_areas))[:5]) or 'ปกติ'}`\n"
    msg1 += f"🌧️ พื้นที่พบฝน: `{', '.join(list(set(rain_provs))[:5]) or 'ไม่พบฝน'}`\n"
    send_tg(msg1)

    # Msg 2: Outdated
    if outdated_list:
        msg2 = "⏳ *รายงานสถานีไม่อัปเดต (แยกตามภูมิภาค)*\n"
        for reg, cfg in REGION_CONFIG.items():
            sts = [x for x in outdated_list if any(p in x['area'] for p in cfg['prov'])]
            if sts:
                msg2 += f"\n📍 *{reg}* (ผู้ดูแล: {cfg['staff']})\n"
                for rs in sts: msg2 += f"• {rs['id']} | {rs['name']} (ขาดหาย: {format_duration(rs['diff'])})\n"
        send_tg(msg2)

    # Msg 3: QA Abnormalities
    if qa_list: send_tg("🚨 *สถานีที่พบข้อมูลผิดปกติ (QA 48h)*\n\n" + "\n".join(qa_list[:20]))

    # Msg 4: Hotspots
    msg4 = f"🔥 *สรุปจุดความร้อน VIIRS (24 ชม.)*\nพบทั้งหมด: `{len(features)}` จุด\n\n"
    msg4 += "\n".join([f"{i+1}. {p}: {c} จุด" for i, (p, c) in enumerate(top5_h)])
    send_tg(msg4)

if __name__ == "__main__":
    main()
