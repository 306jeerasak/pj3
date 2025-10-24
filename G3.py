import threading
import time
import random
import requests
from datetime import datetime, timezone

# ---------------- MT5Bot Simulation Class ----------------
class MT5BotSim:
    # เปลี่ยนไปใช้ 9000/exec ซึ่งเป็น HTTP API ที่รับ SQL Query
    QUESTDB_EXEC_URL = "http://localhost:9000/exec"

    def __init__(self, symbol="EURUSD", n_positions=3):
        self.symbol = symbol
        self.n_positions = n_positions

    # ---------- Simulate Position ----------
    def simulate_position(self, pos_type="buy"):
        """
        สร้าง position ปลอม สำหรับส่ง QuestDB
        """
        class FakePosition:
            def __init__(self, symbol, ticket, pos_type, volume, price_open, price_current, profit):
                self.symbol = symbol
                self.ticket = int(ticket)
                self.type = pos_type
                self.volume = float(volume)
                self.price_open = float(price_open)
                self.price_current = float(price_current)
                self.profit = float(profit)

        # สร้าง ticket ID จากเวลาปัจจุบัน (มิลลิวินาที) + สุ่ม เพื่อให้ไม่ซ้ำ
        ticket = int(time.time() * 1000) % 100000 + random.randint(1, 999)
        volume = round(random.uniform(0.01, 0.05), 2)
        
        # จำลองราคาและกำไร/ขาดทุน
        if "USD" in self.symbol and "XAU" not in self.symbol:
            # สำหรับคู่สกุลเงิน EURUSD, GBPUSD (5 ทศนิยม)
            price_open = round(random.uniform(1.05, 1.30), 5)
            # จำลองการเปลี่ยนแปลงราคา
            price_current = round(price_open + random.uniform(-0.005, 0.005), 5) 
            # คำนวณกำไร: (ราคาปัจจุบัน - ราคาเปิด) * Volume * Contract Size (100,000)
            profit = round((price_current - price_open) * volume * 100000, 2)
        else:  # XAUUSD (1 ทศนิยม)
            price_open = round(random.uniform(1900, 2000), 1)
            # จำลองการเปลี่ยนแปลงราคา
            price_current = round(price_open + random.uniform(-5, 5), 1)
            # คำนวณกำไร: (ราคาปัจจุบัน - ราคาเปิด) * Volume * Contract Size (100)
            profit = round((price_current - price_open) * volume * 100, 2)

        # สำหรับ sell position ให้ profit เป็นค่าตรงข้าม
        if pos_type == "sell":
            profit = -profit

        return FakePosition(self.symbol, ticket, pos_type, volume, price_open, price_current, profit)

    # ---------- QuestDB Logging (ปรับปรุงการจัดการ TIMESTAMP) ----------
    def save_position_to_questdb(self, position):
        """
        บันทึก position ลง QuestDB ด้วย SQL INSERT ผ่าน HTTP API
        *** แก้ไข: QuestDB HTTP SQL API ต้องรับ TIMESTAMP ในรูปแบบ ISO 8601 String ***
        """
        # 1. สร้าง timestamp ในรูปแบบ ISO 8601 string (UTC)
        # ใช้ datetime.now(timezone.utc) เพื่อให้ได้ค่าเวลาที่เป็น UTC โดยมี Timezone information
        timestamp_dt = datetime.now(timezone.utc)
        
        # 2. แปลงเป็น ISO 8601 string ที่ลงท้ายด้วย 'Z' (แสดงว่าเป็น UTC) 
        # รูปแบบ: 'YYYY-MM-DDTHH:mm:ss.SSSSSSZ' (QuestDB รองรับ microsecond)
        # ใช้ .isoformat() และแทนที่ +00:00 ด้วย Z
        timestamp_str = timestamp_dt.isoformat().replace('+00:00', 'Z')
        
        # สร้าง SQL INSERT query
        query = f"""
        INSERT INTO positions
        VALUES(
            '{position.symbol}',
            {position.ticket},
            '{position.type}',
            {position.volume},
            {position.price_open},
            {position.price_current},
            {position.profit},
            '{timestamp_str}'  -- !!! ส่งเป็น String ตามรูปแบบ QuestDB !!!
        )
        """
        
        try:
            # ใช้ params={'query': ...} เพื่อให้ requests จัดการ encoding URL ให้ถูกต้อง
            r = requests.get(self.QUESTDB_EXEC_URL, params={'query': query})
            
            if r.status_code == 200:
                result = r.json()
                if 'error' in result:
                    print(f"[{self.symbol}] ❌ SQL Error: {result.get('error', 'Unknown error')}")
                    # พิมพ์ Query ที่ส่งไปเพื่อ Debug
                    # print(f"Query: {query.strip()}")
                else:
                    print(f"[{self.symbol}] ✅ Position {position.ticket} saved | Profit: {position.profit:9.2f} | Time: {timestamp_str}")
            else:
                print(f"[{self.symbol}] 🛑 HTTP Error: status={r.status_code}, {r.text.strip()[:100]}...")
        except requests.exceptions.ConnectionError as e:
            print(f"[{self.symbol}] 🚨 Exception: Cannot connect to QuestDB at {self.QUESTDB_EXEC_URL}. Is it running?")
            print(f"[{self.symbol}] Error details: {e}")
            time.sleep(5) # หยุดรอ 5 วินาที ก่อนลองใหม่
        except Exception as e:
            print(f"[{self.symbol}] 💥 Unknown Exception: {e}")

    # ---------- Run Simulation Loop ----------
    def run(self):
        print(f"[{self.symbol}] Bot started...")
        while True:
            # สร้างและส่ง Buy/Sell positions ตามจำนวนที่กำหนด
            for _ in range(self.n_positions):
                # 1. สร้าง Buy position
                pos_buy = self.simulate_position("buy")
                self.save_position_to_questdb(pos_buy)
                
                time.sleep(0.5) # หน่วงเล็กน้อยระหว่างแต่ละ position

                # 2. สร้าง Sell position
                pos_sell = self.simulate_position("sell")
                self.save_position_to_questdb(pos_sell)
                
                time.sleep(0.5)

            time.sleep(2) # รอก่อนรอบถัดไป

# ---------------- Main: MultiThread Simulation ----------------
if __name__ == "__main__":
    symbols = ["EURUSD", "XAUUSD", "GBPUSD"]
    bots = []

    print("=" * 60)
    print(" 🤖 MT5 Position Logger Simulation (QuestDB) 📈")
    print("=" * 60)
    print(f"Target QuestDB: {MT5BotSim.QUESTDB_EXEC_URL.split('/exec')[0]}")
    print(f"Symbols: {', '.join(symbols)}")
    print(f"Positions per round: {MT5BotSim(symbols[0]).n_positions} buys + {MT5BotSim(symbols[0]).n_positions} sells per symbol")
    print(f"Total positions/cycle: {len(symbols) * MT5BotSim(symbols[0]).n_positions * 2}")
    print("=" * 60)

    # --- (คำแนะนำการสร้างตาราง) ---
    print("!!! ⚠️ ACTION REQUIRED ⚠️ !!!")
    print("Go to http://127.0.0.1:9000 and run this SQL query FIRST:")
    print("-" * 60)
    print("CREATE TABLE IF NOT EXISTS positions (")
    print("  symbol SYMBOL INDEX,")
    print("  ticket LONG,")
    print("  type SYMBOL,")
    print("  volume DOUBLE,")
    print("  price_open DOUBLE,")
    print("  price_current DOUBLE,")
    print("  profit DOUBLE,")
    print("  timestamp TIMESTAMP")
    print(") TIMESTAMP(timestamp) PARTITION BY DAY;")
    print("-" * 60)
    
    print("\nWaiting 10 seconds to start bots...\n")
    time.sleep(10) # หน่วงเวลาให้อ่าน

    # --- เริ่มต้น Bot ใน Multi-Thread ---
    for sym in symbols:
        bot = MT5BotSim(symbol=sym, n_positions=3)
        t = threading.Thread(target=bot.run, daemon=True)
        t.start()
        bots.append(t)
        time.sleep(0.2) # หน่วงเล็กน้อยก่อนเริ่ม bot ถัดไป

    print("\n\n🎉 ✓ All bots running... Press Ctrl+C to stop.\n")

    # Keep main thread alive
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n\nSimulation stopped.")
        print("🔍 Check your data at: http://127.0.0.1:9000")