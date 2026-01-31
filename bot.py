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

# เกณฑ์การตรวจสอบ (Thresholds)
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

def send_tg(text):
    for cid in TELEGRAM_CHAT_IDS:
        if not cid.strip(): continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": cid.strip(), "text": text, "parse_mode": "Markdown"}, timeout=15)
        except: pass

def summarize_weather_impact(full_text):
    """สรุปข้อความพยากรณ์อากาศยาวๆ ให้เป็น Bullet point สำหรับเจ้าหน้าที่"""
    if not full_text or "ไม่พบข้อมูล" in full_text:
        return "⚠️ ไม่สามารถวิเคราะห์พยากรณ์อากาศได้"
    
    summary = []
    # 1. สรุปสภาวะหลัก (มวลอากาศเย็น/ฝน/หมอก)
    if "มวลอากาศเย็น" in full_text or "ความกดอากาศสูง" in full_text:
        summary.append("🌡️ ความกดอากาศสูงแผ่ปกคลุม (อากาศเย็นลง)")
    if "หมอกในตอนเช้า" in full_text:
        summary.append("🌫️ มีหมอกตอนเช้า/ทัศนวิสัยต่ำ")
    if "ฝนน้อย" in full_text or "ไม่มีฝน" in full_text:
        summary.append("☀️ ฝนน้อย (ไม่มีปัจจัยช่วยชะล้างฝุ่น)")
    elif "ฝนเล็กน้อย" in full_text or "มีฝน" in full_text:
        summary.append("🌧️ มีฝนบางพื้นที่ (ช่วยลดการสะสมฝุ่น)")

    # 2. สรุปการระบายอากาศ
    if "ระบายอากาศอยู่ในเกณฑ์อ่อน" in full_text or "ไม่ดี" in full_text:
        summary.append("🌬️ การระบายอากาศ: *เกณฑ์อ่อนถึงไม่ดี* (เสี่ยงสะสม)")
    elif "ระบายอากาศได้ดี" in full_text:
        summary.append("🌬️ การระบายอากาศ: *เกณฑ์ดี*")

    # 3. แนวโน้มฝุ่น
    if "สะสมของฝุ่นละออง...อยู่ในเกณฑ์ปานกลางถึงค่อนข้างมาก" in full_text or "ค่อนข้างมาก" in full_text:
        summary.append("🔴 แนวโน้มฝุ่น: *สะสมเพิ่มขึ้น/ค่อนข้างมาก*")
    elif "เกณฑ์น้อย" in full_text:
        summary.append("🟢 แนวโน้มฝุ่น: *สะสมน้อย*")

    if not summary:
        return f"📝 {full_text[:150]}..."
        
    return "\n".join([f"• {item}" for item in summary])

def check_qa_issues_48h(station_id):
    """ตรวจสอบ QA แบบละเอียด 48 ชม."""
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
        
        consecutive_missing = 0
        has_large_gap = False
        for val in df['PM25'].tolist():
            if pd.isna(val) or val == -1:
                consecutive_missing += 1
                if consecutive_missing >= MISSING_LIMIT_HRS: has_large_gap = True
            else: consecutive_missing = 0
        if has_large_gap: issues.append(f"Missing(>4h)")
        
        if any(df['PM25'].tail(12).rolling(window=FLATLINE_LIMIT_HRS).std() == 0): issues.append("Flatline")
        if any((df['PM25'] < 0) & (df['PM25'] != -1)): issues.append("Negative")
            
        return ", ".join(issues) if issues else None
    except: return None

def fetch_xml_safe(url, label):
    try:
        res = requests.get(url, headers=HEADERS, timeout=45)
        content = res.content.decode('utf-8-sig').strip()
        return ET.fromstring(content)
    except Exception as e:
        print(f"Error fetching {label}: {e}")
        return None

def main():
    now = get_now_th()
    
    # --- 1. Fetch Data ---
    hourly_raw = requests.get(f"http://air4thai.com/services/getAQI_County.php?key={AIR4THAI_KEY}", headers=HEADERS, timeout=30).json()
    daily_raw = requests.get("http://air4thai.com/forweb/getAQI_JSON.php", headers=HEADERS, timeout=30).json()
    gistda_url = "https://api-gateway.gistda.or.th/api/2.0/resources/features/viirs/1day?limit=3000&offset=0&ct_tn=%E0%B8%A3%E0%B8%B2%E0%B8%8A%E0%B8%AD%E0%B8%B2%E0%B8%93%E0%B8%B2%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3%E0%B9%84%E0%B8%97%E0%B8%A2"
    hotspots_raw = requests.get(gistda_url, headers={**HEADERS, 'API-Key': GISTDA_API_KEY}, timeout=30).json()
    
    # ดึงพยากรณ์รายวัน (Small XML)
    daily_weather_xml = fetch_xml_safe(f"https://data.tmd.go.th/api/DailyForecast/v2/?uid=api&ukey={TMD_DAILY_KEY}", "Daily Forecast")
    weather_3hr_xml = fetch_xml_safe(f"https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey={TMD_3HR_KEY}", "3Hr Weather")

    # --- 2. Processing Air Quality & QA ---
    valid_h = [s for s in hourly_raw if s and isinstance(s, dict) and s.get('hourly_data')]
    pm1h_vals = [float(s['hourly_data']['PM25']) for s in valid_h if s['hourly_data'].get('PM25') is not None and float(s['hourly_data']['PM25']) >= 0]
    pm24h_vals = [float(s['AQILast']['PM25']['value']) for s in daily_raw.get('stations', []) if s and s.get('AQILast', {}).get('PM25', {}).get('value') is not None and float(s['AQILast']['PM25']['value']) >= 0]

    outdated_list, qa_list = [], []
    qa_candidates = sorted(valid_h, key=lambda x: float(x['hourly_data'].get('PM25', 0)), reverse=True)[:15]
    
    for s in valid_h:
        st_id, st_name, area = s['StationID'], s['StationNameTh'], s['AreaNameTh']
        if s.get('last_datetime'):
            try:
                diff = now - datetime.strptime(s['last_datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Bangkok'))
                if diff.total_seconds() > STALE_THRESHOLD_MIN * 60: 
                    outdated_list.append({'id': st_id, 'name': st_name, 'area': area, 'diff': diff})
                    if st_id not in [x['StationID'] for x in qa_candidates]: qa_candidates.append(s)
            except: pass

    for s in qa_candidates:
        issue = check_qa_issues_48h(s['StationID'])
        if issue: qa_list.append(f"• *[{s['StationID']}]* {s['StationNameTh']}\n  ⚠️ ปัญหา: {issue}")

    # --- 3. Weather Analysis ---
    rain_provs, wind_data = [], {}
    if weather_3hr_xml is not None:
        for st in weather_3hr_xml.findall('.//Station'):
            p_node = st.find('Province')
            p = p_node.text.strip() if p_node is not None and p_node.text else "N/A"
            obs = st.find('Observation')
            if obs is not None:
                r_node = obs.find('Rainfall')
                if r_node is not None and r_node.text and float(r_node.text) > 0: rain_provs.append(p)
                w_node = obs.find('WindSpeed')
                if w_node is not None and w_node.text: wind_data[p] = float(w_node.text)

    overall_desc_text = "ไม่พบข้อมูลพยากรณ์อากาศ"
    if daily_weather_xml is not None:
        # แก้ไข Path ให้เข้าถึง OverallDescriptionThai ได้ชัวร์ขึ้น
        desc_node = daily_weather_xml.find('.//OverallDescriptionThai')
        if desc_node is not None and desc_node.text:
            overall_desc_text = desc_node.text.strip().replace('\xa0', ' ')

    # เรียกใช้ฟังก์ชันสรุป Bullet point
    weather_bullets = summarize_weather_impact(overall_desc_text)

    # --- 4. Hotspots ---
    features = hotspots_raw.get('features', [])
    h_provs = {}
    for f in features:
        p = f.get('properties', {}).get('pv_tn', 'N/A')
        h_provs[p] = h_provs.get(p, 0) + 1
    top5_h = sorted(h_provs.items(), key=lambda x: x[1], reverse=True)[:5]

    # --- 5. Beautiful Reporting ---
    
    # Message 1: สรุปภาพรวม
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
    
    msg1 += f"🌤 *สภาวะอากาศและผลกระทบฝุ่น*\n"
    msg1 += f"{weather_bullets}\n\n"
    
    msg1 += f"🔍 *วิเคราะห์ความเสี่ยงรายพื้นที่*\n"
    risk_areas = [p for p, w in wind_data.items() if w < 5 and p in h_provs]
    msg1 += f"📍 *เฝ้าระวังสะสม (ลมนิ่ง+ไฟ):*\n   `{', '.join(list(set(risk_areas))[:5]) or 'สภาวะระบายอากาศปกติ'}`\n"
    msg1 += f"🌧️ *พื้นที่รายงานฝน:* `{', '.join(list(set(rain_provs))[:5]) or 'ไม่มี'}`\n"
    send_tg(msg1)

    # Message 2: รายละเอียดสถานีไม่อัปเดต
    if outdated_list:
        msg2 = "⏳ *สถานีที่หยุดส่งข้อมูล (ปัจจุบัน)*\n"
        msg2 += "━━━━━━━━━━━━━━━━━━━━\n"
        for reg, cfg in REGION_CONFIG.items():
            sts = [x for x in outdated_list if any(p in x['area'] for p in cfg['prov'])]
            if sts:
                msg2 += f"\n📍 *{reg}* ({cfg['staff']})\n"
                for rs in sts:
                    msg2 += f"• `[{rs['id']}]` {rs['name'][:20]}\n  (หยุดส่งข้อมูล: {format_duration(rs['diff'])})\n"
        send_tg(msg2)

    # Message 3: ข้อมูลผิดปกติ (QA)
    if qa_list:
        msg3 = "🚨 *ตรวจพบข้อมูลผิดปกติ (QA 48h)*\n"
        msg3 += "━━━━━━━━━━━━━━━━━━━━\n"
        msg3 += "\n".join(qa_list[:15])
        msg3 += f"\n\n_ตรวจสอบประวัติย้อนหลังเพื่อยืนยันค่าจริง_"
        send_tg(msg3)

    # Message 4: Hotspots
    msg4 = f"🔥 *สรุปจุดความร้อน VIIRS (24 ชม.)*\n"
    msg4 += f"พบทั้งหมด `{len(features):,}` จุด\n"
    msg4 += "━━━━━━━━━━━━━━━━━━━━\n\n"
    msg4 += "🏆 *จังหวัดที่พบจุดสูงสุด*\n"
    for i, (p, c) in enumerate(top5_h, 1):
        msg4 += f"{i}. *{p}* ➔ `{c}` จุด\n"
    send_tg(msg4)

if __name__ == "__main__":
    main()
