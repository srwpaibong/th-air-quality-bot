import os
import requests
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import pandas as pd
import time
import re

# ===== CONFIGURATION =====
AIR4THAI_KEY = os.getenv('AIR4THAI_KEY')
GISTDA_API_KEY = os.getenv('GISTDA_API_KEY')
TMD_3HR_KEY = os.getenv('TMD_3HR_KEY')
TMD_DAILY_KEY = os.getenv('TMD_DAILY_KEY')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',')

# Thresholds
STALE_THRESHOLD_MIN = 80
SPIKE_LIMIT = 50
MISSING_LIMIT_HRS = 4 
FLATLINE_LIMIT_HRS = 4

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

REGION_CONFIG = {
    'ภาคเหนือ': {'prov': ['เชียงราย', 'เชียงใหม่', 'พะเยา', 'แพร่', 'น่าน', 'อุตรดิตถ์', 'ลำปาง', 'ตาก', 'ลำพูน', 'แม่ฮ่องสอน', 'สุโขทัย', 'กำแพงเพชร', 'เพชรบูรณ์', 'พิษณุโลก', 'นครสวรรค์', 'อุทัยธานี'], 'staff': 'พี่ป๊อปปี้'},
    'ภาคกลาง': {'prov': ['กาญจนบุรี', 'สุพรรณบุรี', 'อ่างทอง', 'ชัยนาท', 'สิงห์บุรี', 'ราชบุรี', 'นครปฐม', 'สมุทรสงคราม', 'สระบุรี', 'พระนครศรีอยุธยา', 'ลพบุรี', 'อุทัยธานี'], 'staff': 'พี่ป๊อปปี้'},
    'กรุงเทพฯและปริมณฑล': {'prov': ['กรุงเทพมหานคร', 'สมุทรสาคร', 'นนทบุรี', 'สมุทรปราการ', 'ปทุมธานี', 'นครปฐม'], 'staff': 'พี่ป๊อปปี้'},
    'ภาคใต้': {'prov': ['ชุมพร', 'ระนอง', 'พังงา', 'ภูเก็ต', 'สุราษฎร์ธานี', 'นครศรีธรรมราช', 'กระบี่', 'ตรัง', 'พัทลุง', 'สตูล', 'สงขลา', 'ปัตตานี', 'ยะลา', 'นราธิวาส', 'ประจวบคีรีขันธ์'], 'staff': 'พี่หน่อย'},
    'ภาคตะวันออกเฉียงเหนือ': {'prov': ['ขอนแก่น', 'กาฬสินธุ์', 'ชัยภูมิ', 'นครพนม', 'นครราชสีมา', 'บึงกาฬ', 'บุรีรัมย์', 'มหาสารคาม', 'มุกดาหาร', 'ยโสธร', 'ร้อยเอ็ด', 'ศรีสะเกษ', 'สกลนคร', 'สุรินทร์', 'หนองคาย', 'หนองบัวลำภู', 'อำนาจเจริญ', 'อุดรธานี', 'อุบลราชธานี', 'เลย'], 'staff': 'พี่หน่อย'},
    'ภาคตะวันออก': {'prov': ['นครนายก', 'ฉะเชิงเทรา', 'ปราจีนบุรี', 'สระแก้ว', 'ชลบุรี', 'ระยอง', 'จันทบุรี', 'ตราด'], 'staff': 'พี่ฟรังก์'}
}

def get_now_th():
    return datetime.now(pytz.timezone('Asia/Bangkok'))

def get_pm25_icon(val):
    if val <= 15: return "🔵"
    if val <= 25: return "🟢"
    if val <= 37.5: return "🟡"
    if val <= 75: return "🟠"
    return "🔴"

def format_duration(diff):
    days = diff.days
    hours = diff.seconds // 3600
    if days > 0: return f"{days}ว {hours}ชม"
    return f"{hours}ชม"

def extract_province(area_th):
    if not area_th: return "ไม่ระบุ"
    parts = area_th.split(',')
    if len(parts) > 1:
        prov = parts[-1].strip().replace('จังหวัด', '').replace('จ.', '')
        if "กรุงเทพ" in prov: return "กรุงเทพฯ"
        return prov
    return area_th.strip()

def send_tg(text):
    for cid in TELEGRAM_CHAT_IDS:
        if not cid.strip(): continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": cid.strip(), "text": text, "parse_mode": "Markdown"}, timeout=15)
        except: pass

def summarize_weather_impact(full_text):
    if not full_text or "ไม่พบข้อมูล" in full_text:
        return "⚠️ ข้อมูลพยากรณ์อากาศจาก TMD ไม่พร้อมใช้งาน"
    
    summary = []
    if any(k in full_text for k in ["มวลอากาศเย็น", "ความกดอากาศสูง"]):
        summary.append("🌡️ *สภาวะอากาศ:* มวลอากาศเย็นแผ่ปกคลุม (อากาศนิ่ง/เย็นลง)")
    if "หมอกในตอนเช้า" in full_text or "หมอกหนา" in full_text:
        summary.append("🌫️ *ทัศนวิสัย:* มีหมอกตอนเช้า (ระวังเพดานอากาศต่ำ)")
    if any(k in full_text for k in ["ฝนน้อย", "ไม่มีฝน"]):
        summary.append("☀️ *ปัจจัยชะล้าง:* ฝนน้อย (เสี่ยงสะสมฝุ่นเพิ่ม)")
    elif any(k in full_text for k in ["มีฝน", "ฝนฟ้าคะนอง"]):
        summary.append("🌧️ *ปัจจัยชะล้าง:* มีฝนบางพื้นที่ (ช่วยลดการสะสม)")
    if any(k in full_text for k in ["ระบายอากาศอยู่ในเกณฑ์อ่อน", "ไม่ดี", "ระบายอากาศได้ไม่ดี"]):
        summary.append("🌬️ *การระบายอากาศ:* เกณฑ์อ่อน/ไม่ดี (ปัจจัยลบ)")
    if any(k in full_text for k in ["สะสม...อยู่ในเกณฑ์ปานกลางถึงค่อนข้างมาก", "สะสมค่อนข้างมาก"]):
        summary.append("🔴 *แนวโน้มฝุ่น:* คาดการณ์สะสมเพิ่มขึ้น (เฝ้าระวังพิเศษ)")

    return "\n".join([f"• {item}" for item in summary]) if summary else f"📝 {full_text[:200]}..."

def check_qa_issues_48h(station_id, is_currently_outdated):
    try:
        now = get_now_th()
        edate = now.strftime('%Y-%m-%d')
        sdate = (now - timedelta(days=2)).strftime('%Y-%m-%d')
        url = f"http://air4thai.com/forweb/getHistoryData.php?stationID={station_id}&param=PM25&type=hr&sdate={sdate}&edate={edate}&stime=00&etime=23"
        res = requests.get(url, headers=HEADERS, timeout=20)
        data = res.json().get('stations', [{}])[0].get('data', [])
        if not data: return None
        
        df = pd.DataFrame(data)
        df['PM25'] = pd.to_numeric(df['PM25'], errors='coerce')
        issues = []
        if any(df['PM25'].diff().abs() > SPIKE_LIMIT): issues.append("Spike")
        if not is_currently_outdated:
            consecutive_missing = 0
            has_history_gap = False
            for val in df['PM25'].iloc[:-1].tolist():
                if pd.isna(val) or val == -1:
                    consecutive_missing += 1
                    if consecutive_missing >= MISSING_LIMIT_HRS: has_history_gap = True
                else: consecutive_missing = 0
            if has_history_gap: issues.append(f"Missing(>4h)")
        if any(df['PM25'].tail(12).rolling(window=FLATLINE_LIMIT_HRS).std() == 0): issues.append("Flatline")
        if any((df['PM25'] < 0) & (df['PM25'] != -1)): issues.append("Negative")
        return ", ".join(issues) if issues else None
    except: return None

def safe_fetch_json(url, label, headers=None, timeout=30):
    """ฟังก์ชันดึงข้อมูล JSON พร้อมระบบ Retry และ Fault Tolerance"""
    for attempt in range(3):
        try:
            res = requests.get(url, headers=headers or HEADERS, timeout=timeout)
            if res.status_code == 200:
                return res.json()
        except Exception as e:
            print(f"[{label}] Attempt {attempt+1} failed: {e}")
            if attempt < 2: time.sleep(10)
    return None

def fetch_xml_safe(url, label, timeout=60):
    """ขยายเวลา Timeout สำหรับ TMD XML"""
    try:
        res = requests.get(url, headers=HEADERS, timeout=timeout)
        if res.status_code != 200: return None
        content = res.content.decode('utf-8-sig').strip()
        return ET.fromstring(content)
    except Exception as e:
        print(f"Error fetching {label}: {e}")
        return None

def main():
    now = get_now_th()
    print(f"=== Job Started at {now} ===")
    
    # --- 1. Fetch Data ---
    hourly_raw = safe_fetch_json(f"http://air4thai.com/services/getAQI_County.php?key={AIR4THAI_KEY}", "Air4Thai Hourly")
    daily_raw = safe_fetch_json("http://air4thai.com/forweb/getAQI_JSON.php", "Air4Thai Daily")
    
    # เพิ่ม Limit เป็น 5000 เผื่อวันไฟเยอะ แต่จะใช้โค้ดกรองเฉพาะไทยอีกครั้ง
    gistda_url = "https://api-gateway.gistda.or.th/api/2.0/resources/features/viirs/1day?limit=5000&offset=0&ct_tn=%E0%B8%A3%E0%B8%B2%E0%B8%8A%E0%B8%AD%E0%B8%B2%E0%B8%93%E0%B8%B2%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3%E0%B9%84%E0%B8%97%E0%B8%A2"
    hotspots_raw = safe_fetch_json(gistda_url, "GISTDA", headers={**HEADERS, 'API-Key': GISTDA_API_KEY}, timeout=60)
    
    daily_weather_xml = fetch_xml_safe(f"https://data.tmd.go.th/api/DailyForecast/v2/?uid=api&ukey={TMD_DAILY_KEY}", "Daily Forecast", timeout=60)
    weather_3hr_xml = fetch_xml_safe(f"https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey={TMD_3HR_KEY}", "3Hr Weather", timeout=90)

    if not hourly_raw or not daily_raw:
        send_tg("❌ ไม่สามารถดึงข้อมูลคุณภาพอากาศหลักได้ บอทหยุดทำงานชั่วคราว")
        return

    # --- 2. Processing Air Quality & QA ---
    valid_h = [s for s in hourly_raw if s and isinstance(s, dict) and s.get('hourly_data')]
    pm1h_vals = [float(s['hourly_data']['PM25']) for s in valid_h if s['hourly_data'].get('PM25') is not None and float(s['hourly_data']['PM25']) >= 0]
    pm24h_vals = [float(s['AQILast']['PM25']['value']) for s in daily_raw.get('stations', []) if s and s.get('AQILast', {}).get('PM25', {}).get('value') is not None and float(s['AQILast']['PM25']['value']) >= 0]

    outdated_list, qa_list, outdated_ids = [], [], set()
    for s in valid_h:
        st_id, area = s['StationID'], s['AreaNameTh']
        prov = extract_province(area)
        if s.get('last_datetime'):
            try:
                diff = now - datetime.strptime(s['last_datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Bangkok'))
                if diff.total_seconds() > STALE_THRESHOLD_MIN * 60: 
                    outdated_list.append({'id': st_id, 'name': s['StationNameTh'], 'prov': prov, 'diff': diff})
                    outdated_ids.add(st_id)
            except: pass

    qa_candidates = sorted(valid_h, key=lambda x: float(x['hourly_data'].get('PM25', 0)), reverse=True)[:15]
    for s in qa_candidates:
        st_id = s['StationID']
        issue = check_qa_issues_48h(st_id, st_id in outdated_ids)
        if issue:
            prov = extract_province(s['AreaNameTh'])
            qa_list.append(f"• *[{st_id}]* {s['StationNameTh']} ({prov})\n  ⚠️ ปัญหา: {issue}")

    # --- 3. Weather ---
    rain_provs, wind_data = [], {}
    if weather_3hr_xml is not None:
        for st in weather_3hr_xml.findall('.//Station'):
            p = st.find('Province').text.strip() if st.find('Province') is not None else "N/A"
            obs = st.find('Observation')
            if obs is not None:
                r, w = obs.find('Rainfall').text, obs.find('WindSpeed').text
                if r and float(r) > 0: rain_provs.append(p)
                if w: wind_data[p] = float(w)

    overall_desc_text = "ไม่พบข้อมูลพยากรณ์อากาศ"
    if daily_weather_xml is not None:
        desc_node = daily_weather_xml.find('.//OverallDescriptionThai')
        if desc_node is None: desc_node = daily_weather_xml.find('.//DailyForecast/OverallDescriptionThai')
        if desc_node is not None and desc_node.text:
            overall_desc_text = desc_node.text.strip().replace('\xa0', ' ')

    weather_bullets = summarize_weather_impact(overall_desc_text)

    # --- 4. Hotspots (Fixing the count to match official Thailand report) ---
    all_features = hotspots_raw.get('features', []) if hotspots_raw else []
    
    # กรองเฉพาะจุดความร้อนที่อยู่ในประเทศไทยจริงๆ (เช็ค ct_tn)
    th_features = [f for f in all_features if f.get('properties', {}).get('ct_tn') in ['ไทย', 'ราชอาณาจักรไทย']]
    
    h_provs = {}
    for f in th_features:
        p = f.get('properties', {}).get('pv_tn', 'ไม่ระบุ')
        h_provs[p] = h_provs.get(p, 0) + 1
    top5_h = sorted(h_provs.items(), key=lambda x: x[1], reverse=True)[:5]

    # --- 5. Reporting ---
    msg1 = f"📡 *รายงานคุณภาพอากาศประเทศไทย*\n"
    msg1 += f"📅 {now.strftime('%d/%m/%Y')} | 🕒 {now.strftime('%H:%M')} น.\n"
    msg1 += f"━━━━━━━━━━━━━━━━━━━━\n\n"
    msg1 += f"{get_pm25_icon(max(pm1h_vals) if pm1h_vals else 0)} *PM2.5 รายชั่วโมง*\n"
    msg1 += f"┗  `{min(pm1h_vals) if pm1h_vals else 0} - {max(pm1h_vals) if pm1h_vals else 0}` µg/m³\n\n"
    msg1 += f"🗓 *PM2.5 เฉลี่ย 24 ชม.*\n"
    msg1 += f"┗  `{min(pm24h_vals) if pm24h_vals else 0} - {max(pm24h_vals) if pm24h_vals else 0}` µg/m³\n\n"
    msg1 += f"📊 *สถานะระบบสถานี*\n"
    msg1 += f"┣  ⚠️ ไม่อัปเดต: `{len(outdated_list)}` แห่ง\n"
    msg1 += f"┗  🚨 ข้อมูลผิดปกติ: `{len(qa_list)}` แห่ง\n\n"
    msg1 += f"🌤 *สภาวะอากาศและผลกระทบฝุ่น*\n{weather_bullets}\n\n"
    msg1 += f"🔍 *วิเคราะห์ความเสี่ยงรายพื้นที่*\n"
    risk_areas = [p for p, w in wind_data.items() if w < 5 and p in h_provs]
    msg1 += f"📍 *เฝ้าระวังสะสม (ลมนิ่ง+ไฟ):*\n   `{', '.join(list(set(risk_areas))[:5]) or 'สภาวะระบายอากาศปกติ'}`\n"
    msg1 += f"🌧️ *พื้นที่รายงานฝน:* `{', '.join(list(set(rain_provs))[:5]) or 'ไม่มี'}`\n"
    send_tg(msg1)

    if outdated_list:
        msg2 = "⏳ *สถานีที่หยุดส่งข้อมูล (ปัจจุบัน)*\n━━━━━━━━━━━━━━━━━━━━\n"
        for reg, cfg in REGION_CONFIG.items():
            sts = [x for x in outdated_list if any(p in x['prov'] for p in cfg['prov'])]
            if sts:
                msg2 += f"\n📍 *{reg}* ({cfg['staff']})\n"
                for rs in sts: msg2 += f"• `[{rs['id']}]` {rs['name']} ({rs['prov']})\n  (หยุดส่ง: {format_duration(rs['diff'])})\n"
        send_tg(msg2)

    if qa_list:
        msg3 = "🚨 *ตรวจพบข้อมูลผิดปกติ (QA 48h)*\n━━━━━━━━━━━━━━━━━━━━\n"
        msg3 += "\n".join(qa_list[:15]) + f"\n\n_ตรวจสอบประวัติย้อนหลังเพื่อความต่อเนื่อง_"
        send_tg(msg3)

    msg4 = f"🔥 *สรุปจุดความร้อน VIIRS - ประเทศไทย*\n"
    msg4 += f"ประจำวันที่: {(now - timedelta(days=1)).strftime('%d/%m/%Y')}\n"
    msg4 += f"พบทั้งหมด `{len(th_features):,}` จุด\n"
    msg4 += "━━━━━━━━━━━━━━━━━━━━\n\n"
    if not hotspots_raw: msg4 += "⚠️ _ไม่สามารถดึงข้อมูล GISTDA ได้ในรอบนี้_\n"
    for i, (p, c) in enumerate(top5_h, 1): msg4 += f"{i}. *{p}* ➔ `{c}` จุด\n"
    send_tg(msg4)

if __name__ == "__main__":
    main()
