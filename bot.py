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
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',')

# เกณฑ์การตรวจสอบ (Thresholds)
STALE_THRESHOLD_MIN = 80
SPIKE_LIMIT = 50
MISSING_LIMIT_HRS = 5
FLATLINE_LIMIT_HRS = 4

# Headers เพื่อป้องกันการโดนบล็อก (User-Agent)
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

def send_tg(text):
    for cid in TELEGRAM_CHAT_IDS:
        if not cid.strip(): continue
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": cid.strip(), "text": text, "parse_mode": "Markdown"}, timeout=15)
        except Exception as e:
            print(f"Error sending to {cid}: {e}")

def check_qa_issues_48h(station_id):
    """ตรวจสอบ QA ย้อนหลัง 48 ชม. โดยใช้ลิงก์สำหรับหน้าเว็บ (No Key)"""
    try:
        now = get_now_th()
        edate = now.strftime('%Y-%m-%d')
        sdate = (now - timedelta(days=2)).strftime('%Y-%m-%d')
        
        # ใช้ลิงก์ getHistoryData (No Key) ตามที่ได้รับข้อมูลมา
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
        
        # 1. Spike Check (> 50 มคก./ลบ.ม. จากชม.ก่อนหน้า)
        # ใช้ diff() หาผลต่าง และ abs() เพื่อดูทั้งขาขึ้นและขาลงที่ผิดปกติ
        df['diff'] = df['PM25'].diff()
        if any(df['diff'] > SPIKE_LIMIT):
            issues.append(f"Spike 📈")

        # 2. Missing Data (> 5 ชม. ต่อเนื่อง)
        # เช็คจากข้อมูล 24 ชม. ล่าสุด
        recent_pm25 = df['PM25'].tail(24).tolist()
        consecutive_missing = 0
        max_missing = 0
        for v in recent_pm25:
            if pd.isna(v) or v == -1:
                consecutive_missing += 1
                max_missing = max(max_missing, consecutive_missing)
            else:
                consecutive_missing = 0
        if max_missing >= MISSING_LIMIT_HRS:
            issues.append(f"Missing {max_missing}h ❓")

        # 3. Flatline (ค่านิ่งไม่ขยับต่อเนื่องเกิน 4 ชม.)
        # เช็ค Rolling standard deviation เป็น 0
        if any(df['PM25'].rolling(window=FLATLINE_LIMIT_HRS).std() == 0):
            issues.append(f"Flatline {FLATLINE_LIMIT_HRS}h 📏")

        # 4. Negative Values (ค่าติดลบที่ไม่ใช่ -1)
        if any((df['PM25'] < 0) & (df['PM25'] != -1)):
            issues.append("Negative ⚙️")

        return ", ".join(issues) if issues else None
    except Exception as e:
        print(f"QA Error for {station_id}: {e}")
        return None

def main():
    now = get_now_th()
    print(f"Starting process at {now}")

    # --- 1. Fetch Basic Data ---
    # ข้อมูลรายชั่วโมง (ใช้ Key)
    hourly_raw = requests.get(f"http://air4thai.com/services/getAQI_County.php?key={AIR4THAI_KEY}", headers=HEADERS, timeout=25).json()
    # ข้อมูลเฉลี่ย 24 ชม. (No Key)
    daily_raw = requests.get("http://air4thai.com/forweb/getAQI_JSON.php", headers=HEADERS, timeout=25).json()
    # ข้อมูลจุดความร้อน (VIIRS)
    gistda_url = "https://api-gateway.gistda.or.th/api/2.0/resources/features/viirs/1day?limit=1000&offset=0&ct_tn=%E0%B8%A3%E0%B8%B2%E0%B8%8A%E0%B8%AD%E0%B8%B2%E0%B8%93%E0%B8%B2%E0%B8%88%E0%B8%B1%E0%B8%81%E0%B8%A3%E0%B9%84%E0%B8%97%E0%B8%A2"
    hotspots_raw = requests.get(gistda_url, headers={**HEADERS, 'API-Key': GISTDA_API_KEY}, timeout=25).json()
    # ข้อมูลสภาพอากาศ (TMD XML)
    tmd_url = f"https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey={TMD_3HR_KEY}"
    tmd_res = requests.get(tmd_url, headers=HEADERS, timeout=25)
    weather_root = ET.fromstring(tmd_res.content)

    # --- 2. Processing Air Quality ---
    v1h = [float(s['hourly_data']['PM25']) for s in hourly_raw if s.get('hourly_data', {}).get('PM25') and float(s['hourly_data']['PM25']) >= 0]
    v24h = [float(s['AQILast']['PM25']['value']) for s in daily_raw.get('stations', []) if s.get('AQILast', {}).get('PM25', {}).get('value') and float(s['AQILast']['PM25']['value']) >= 0]
    
    outdated_list = []
    qa_list = []
    
    # วนลูปตรวจสอบทุกสถานีในข้อมูลรายชั่วโมง
    for s in hourly_raw:
        st_id, st_name, area = s['StationID'], s['StationNameTh'], s['AreaNameTh']
        
        # 2.1 ตรวจสอบสถานีไม่อัปเดต
        if s.get('last_datetime'):
            last_dt = datetime.strptime(s['last_datetime'], "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.timezone('Asia/Bangkok'))
            diff = now - last_dt
            if diff.total_seconds() > STALE_THRESHOLD_MIN * 60:
                outdated_list.append({'id': st_id, 'name': st_name, 'area': area, 'diff': diff, 'last': s['last_datetime']})
        
        # 2.2 ตรวจสอบ QA 48h (สุ่มตรวจหรือเลือกสถานีที่มีโอกาสผิดปกติเพื่อประหยัดเวลา Action)
        # ในที่นี้จะเช็คสถานีที่มีค่าปัจจุบันสูงผิดปกติ (>150) หรือติดลบ หรือค่า Error
        cur_val = float(s.get('hourly_data', {}).get('PM25', -1))
        if cur_val > 150 or cur_val < -1 or st_id in ["05t", "12t"]: # เพิ่มไอดีสถานีที่ต้องการเฝ้าระวังพิเศษที่นี่
            qa_issue = check_qa_issues_48h(st_id)
            if qa_issue:
                qa_list.append(f"• {st_id} | {st_name}: {qa_issue}")

    # --- 3. Processing Weather & Hotspots ---
    # หาจังหวัดที่มีฝน
    rain_provs = []
    wind_data = {}
    for st in weather_root.findall('.//Station'):
        prov = st.find('Province').text.strip()
        rain = st.find('.//Observation/Rainfall').text
        wind = st.find('.//Observation/WindSpeed').text
        if rain and float(rain) > 0: rain_provs.append(prov)
        wind_data[prov] = float(wind) if wind else 0

    # จัดอันดับจุดความร้อน
    hotspots = hotspots_raw.get('features', [])
    h_provs = {}
    for h in hotspots:
        p = h['properties'].get('pv_tn', 'N/A')
        h_provs[p] = h_provs.get(p, 0) + 1
    top5_h = sorted(h_provs.items(), key=lambda x: x[1], reverse=True)[:5]

    # --- 4. Building Reports ---
    
    # รายงานที่ 1: ภาพรวมประเทศและการวิเคราะห์
    msg1 = f"🌏 *สรุปคุณภาพอากาศประเทศไทย*\n"
    msg1 += f"อัพเดทข้อมูล ณ วันที่: {now.strftime('%d/%m/%Y')} เวลา {now.strftime('%H:%M')} น.\n\n"
    msg1 += f"📊 PM2.5 (ราย 1 ชม.): `{min(v1h)}-{max(v1h)}` µg/m³\n"
    msg1 += f"📊 PM2.5 (เฉลี่ย 24 ชม.): `{min(v24h)}-{max(v24h)}` µg/m³\n\n"
    msg1 += f"⚠️ สถานีไม่อัปเดต: `{len(outdated_list)}` สถานี\n"
    msg1 += f"🚨 สถานีข้อมูลผิดปกติ: `{len(qa_list)}` สถานี\n\n"
    
    # วิเคราะห์พื้นที่เฝ้าระวัง
    msg1 += f"🔍 *บทวิเคราะห์และการเฝ้าระวัง:*\n"
    risk_areas = [p for p, w in wind_data.items() if w < 5 and p in h_provs]
    msg1 += f"📍 พื้นที่ลมนิ่ง+ไฟสูง: `{', '.join(list(set(risk_areas))[:5]) or 'ไม่พบพื้นที่วิกฤต'}`\n"
    msg1 += f"🌧️ พื้นที่ที่พบฝน: `{', '.join(list(set(rain_provs))[:5]) or 'ไม่พบรายงานฝน'}`\n"
    send_tg(msg1)

    # รายงานที่ 2: รายละเอียดสถานีไม่อัปเดต (ภูมิภาค)
    if outdated_list:
        msg2 = "⏳ *รายงานสถานีไม่อัปเดต (แยกตามภูมิภาค)*\n"
        for reg, cfg in REGION_CONFIG.items():
            sts = [x for x in outdated_list if any(p in x['area'] for p in cfg['prov'])]
            if sts:
                msg2 += f"\n📍 *{reg}* (ผู้ดูแล: {cfg['staff']})\n"
                for rs in sts:
                    d, h = rs['diff'].days, rs['diff'].seconds // 3600
                    msg2 += f"• {rs['id']} | {rs['name']}\n   (หยุดส่งข้อมูล: {d} วัน {h} ชม.)\n"
        send_tg(msg2)

    # รายงานที่ 3: ข้อมูลผิดปกติ (QA)
    if qa_list:
        send_tg("🚨 *สถานีที่พบข้อมูลผิดปกติ (QA 48h)*\n\n" + "\n".join(qa_list[:20]))

    # รายงานที่ 4: สรุปจุดความร้อน
    msg4 = f"🔥 *สรุปจุดความร้อน VIIRS (24 ชม.)*\n"
    msg4 += f"พบทั้งหมด: `{len(hotspots)}` จุด\n\n"
    msg4 += "🏆 *5 จังหวัดที่พบจุดความร้อนสูงสุด:*\n"
    for i, (p, c) in enumerate(top5_h, 1):
        msg4 += f"{i}. {p}: `{c}` จุด\n"
    send_tg(msg4)

if __name__ == "__main__":
    main()
