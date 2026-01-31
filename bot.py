import os
import requests
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import pandas as pd

# ===== CONFIGURATION (Environment Variables) =====
AIR4THAI_KEY = os.getenv('AIR4THAI_KEY')
GISTDA_API_KEY = os.getenv('GISTDA_API_KEY')
TMD_3HR_KEY = os.getenv('TMD_3HR_KEY') # Key ตัวใหม่สำหรับ Weather3Hours
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',')

# Thresholds สำหรับการตรวจสอบ
STALE_THRESHOLD_MIN = 80
SPIKE_LIMIT = 50
MISSING_LIMIT_HRS = 5
FLATLINE_LIMIT_HRS = 4

# ข้อมูลภูมิภาคและผู้รับผิดชอบ
REGION_CONFIG = {
    'ภาคเหนือ': {'prov': ['เชียงราย', 'เชียงใหม่', 'พะเยา', 'แพร่', 'น่าน', 'อุตรดิตถ์', 'ลำปาง', 'ตาก', 'ลำพูน', 'แม่ฮ่องสอน', 'สุโขทัย', 'กำแพงเพชร', 'เพชรบูรณ์', 'พิษณุโลก', 'นครสวรรค์', 'อุทัยธานี'], 'staff': 'พี่ป๊อปปี้'},
    'ภาคกลาง': {'prov': ['กาญจนบุรี', 'สุพรรณบุรี', 'อ่างทอง', 'ชัยนาท', 'สิงห์บุรี', 'ราชบุรี', 'สระบุรี', 'พระนครศรีอยุธยา', 'ลพบุรี', 'เพชรบุรี', 'สมุทรสงคราม', 'ประจวบคีรีขันธ์'], 'staff': 'พี่ป๊อปปี้'},
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
    if days > 0:
        return f"{days} วัน {hours} ชม."
    return f"{hours} ชม."

def send_tg(text):
    for cid in TELEGRAM_CHAT_IDS:
        if not cid.strip(): continue
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                      json={"chat_id": cid.strip(), "text": text, "parse_mode": "Markdown"})

def check_qa_issues(station_id):
    """ฟังก์ชันตรวจสอบ QA ย้อนหลัง 48 ชม."""
    try:
        now = get_now_th()
        end_date = now.strftime('%Y-%m-%d')
        start_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
        url = f"http://air4thai.com/services/getStationHistory.php?stationID={station_id}&param=PM25&type=hr&startdate={start_date}&enddate={end_date}&key={AIR4THAI_KEY}"
        res = requests.get(url, timeout=10).json()
        data = res.get('stationHistory', [{}])[0].get('data', [])
        if not data: return None
        
        df = pd.DataFrame(data)
        df['PM25'] = pd.to_numeric(df['PM25'], errors='coerce')
        issues = []
        
        # 1. Spike Check (> 50)
        df['diff'] = df['PM25'].diff().abs()
        if any(df['diff'] > SPIKE_LIMIT): issues.append(f"Spike >{SPIKE_LIMIT}")
        
        # 2. Missing Check (ล่าสุดหายไป 5 ชม. ขึ้นไป)
        if df['PM25'].tail(12).isna().sum() >= MISSING_LIMIT_HRS: issues.append("Missing ❓")
        
        # 3. Flatline (ค่านิ่งต่อเนื่อง 4 ชม.)
        if any(df['PM25'].rolling(window=FLATLINE_LIMIT_HRS).std() == 0): issues.append("Flatline 📏")
        
        # 4. Negative values (ไม่ใช่ -1)
        if any(df['PM25'] < -1): issues.append("Negative Value ⚙️")
        
        return ", ".join(issues) if issues else None
    except: return None

def main():
    now = get_now_th()
    date_text = now.strftime('%d %B %Y').replace("January", "มกราคม").replace("February", "กุมภาพันธ์") # เพิ่ม Mapping เดือนได้ตามชอบ
    time_text = now.strftime('%H:%M')

    # --- Fetch Data ---
    hourly = requests.get(f"http://air4thai.com/services/getAQI_County.php?key={AIR4THAI_KEY}").json()
    daily = requests.get("http://air4thai.com/forweb/getAQI_JSON.php").json().get('stations', [])
    gistda_url = "https://api-gateway.gistda.or.th/api/2.0/resources/features/viirs/1day?limit=1000&offset=0&ct_tn=%E0%B8%A3%E0%B8%B2%E0%B8%8A%E0%B8%AD%E0%B8%B2%E0%B8%93%E0%B8%B2%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3%E0%B9%84%E0%B8%97%E0%B8%A2"
    hotspots = requests.get(gistda_url, headers={'API-Key': GISTDA_API_KEY}).json().get('features', [])
    
    # ✅ ใช้ Key ตัวใหม่สำหรับ TMD 3-Hour
    tmd_url = f"https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey={TMD_3HR_KEY}"
    tmd_res = requests.get(tmd_url)
    weather_root = ET.fromstring(tmd_res.content)

    # --- 1. วิเคราะห์ PM2.5 & ข้อมูลผิดปกติ ---
    v1h = [float(s['hourly_data']['PM25']) for s in hourly if s.get('hourly_data', {}).get('PM25') and float(s['hourly_data']['PM25']) >= 0]
    v24h = [float(s['AQILast']['PM25']['value']) for s in daily if s.get('AQILast', {}).get('PM25', {}).get('value') and float(s['AQILast']['PM25']['value']) >= 0]
    
    outdated_list = []
    qa_list = []
    for s in hourly:
        st_id, st_name, area = s['StationID'], s['StationNameTh'], s['AreaNameTh']
        # ไม่อัปเดต
        if s.get('last_datetime'):
            last_dt = datetime.strptime(s['last_datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Bangkok'))
            diff = now - last_dt
            if diff.total_seconds() > STALE_THRESHOLD_MIN * 60:
                outdated_list.append({'id': st_id, 'name': st_name, 'area': area, 'diff': diff, 'last': s['last_datetime']})
        
        # เช็ค QA (เลือกเช็คเฉพาะที่พุ่งสูงหรือติดลบเพื่อความรวดเร็ว)
        val = float(s['hourly_data'].get('PM25', -1))
        if val > 150 or val < -1:
            issue = check_qa_issues(st_id)
            if issue: qa_list.append(f"• {st_id} | {st_name}: {issue}")

    # --- 2. วิเคราะห์สภาพอากาศและจุดความร้อน ---
    rain_provs = [st.find('Province').text.strip() for st in weather_root.findall('.//Station') if float(st.find('.//Observation/Rainfall').text or 0) > 0]
    calm_provs = [st.find('Province').text.strip() for st in weather_root.findall('.//Station') if float(st.find('.//Observation/WindSpeed').text or 0) < 5]
    
    h_provs = {}
    for h in hotspots:
        p = h['properties'].get('pv_tn', 'N/A')
        h_provs[p] = h_provs.get(p, 0) + 1
    top5_h = sorted(h_provs.items(), key=lambda x: x[1], reverse=True)[:5]

    # --- 3. สร้างรายงาน ---
    
    # Message 1: สรุปภาพรวม
    msg1 = f"🌏 *สรุปสถานการณ์คุณภาพอากาศประเทศไทย*\n"
    msg1 += f"อัพเดทข้อมูล ณ วันที่: {date_text} เวลา {time_text}\n\n"
    msg1 += f"📊 PM2.5 (1h): `{min(v1h)}-{max(v1h)}` | (24h): `{min(v24h)}-{max(v24h)}` µg/m³\n"
    msg1 += f"⚠️ ไม่อัปเดต: `{len(outdated_list)}` | ผิดปกติ (QA): `{len(qa_list)}` สถานี\n\n"
    msg1 += f"🔍 *บทวิเคราะห์:* \n"
    risk_area = list(set(calm_provs) & set(h_provs.keys()))
    msg1 += f"📍 เฝ้าระวัง (ลมนิ่ง+ไฟ): `{', '.join(risk_area[:5]) or 'ไม่พบพื้นที่วิกฤต'}`\n"
    msg1 += f"🌧️ พื้นที่พบฝน: `{', '.join(list(set(rain_provs))[:5]) or 'ไม่พบรายงานฝน'}`\n"
    send_tg(msg1)

    # Message 2: สถานีไม่อัปเดต (ภูมิภาค)
    if outdated_list:
        msg2 = f"⏳ *รายงานสถานีไม่อัปเดต*\n"
        msg2 += f"อัพเดท ณ วันที่: {date_text} เวลา {time_text}\n"
        for reg, cfg in REGION_CONFIG.items():
            sts = [x for x in outdated_list if any(p in x['area'] for p in cfg['prov'])]
            if sts:
                msg2 += f"\n📍 *{reg}* ({cfg['staff']})\n"
                for rs in sts:
                    msg2 += f"• {rs['id']} | {rs['name']} (ขาดหาย: {format_duration(rs['diff'])})\n"
        send_tg(msg2)

    # Message 3: ข้อมูลผิดปกติ
    if qa_list:
        send_tg(f"🚨 *สถานีที่พบข้อมูลผิดปกติ (QA 48h)*\n\n" + "\n".join(qa_list[:20]))

    # Message 4: จุดความร้อน
    msg4 = f"🔥 *สรุปจุดความร้อนประจำวัน (VIIRS)*\n"
    msg4 += f"รวมทั้งหมด: `{len(hotspots)}` จุด\n\n"
    msg4 += "🏆 *5 จังหวัดสูงสุด:*\n"
    for i, (p, c) in enumerate(top5_h, 1):
        msg4 += f"{i}. {p}: `{c}` จุด\n"
    send_tg(msg4)

if __name__ == "__main__":
    main()
