# coding: utf-8
import asyncio, websockets, json, time, requests, csv, os
from datetime import datetime

# ───── CẤU HÌNH ──────────────────────────────────────────────
RAW_SYMBOLS = [
    'ENAUSDT'
]

def kucoin_futures_code(spot: str) -> str:
    """BTCUSDT  → BTCUSDTM (perpetual)"""
    return spot.replace('USDT', 'USDTM')

SYMBOLS   = [kucoin_futures_code(s) for s in RAW_SYMBOLS]
BATCH_SIZE, LIMIT = 25, 50                              # ghi 25-row / độ sâu 50
initialized_files = set()

header = ['timestamp', 'symbol']
for i in range(LIMIT):
    header += [f'bid_price_{i:02d}', f'ask_price_{i:02d}',
               f'bid_volume_{i:02d}', f'ask_volume_{i:02d}']

# ───── HÀM TIỆN ÍCH ──────────────────────────────────────────
def get_kucoin_ws_token():
    """Lấy public-token & endpoint cho WebSocket Futures."""
    url = "https://api-futures.kucoin.com/api/v1/bullet-public"
    try:
        r = requests.post(url, timeout=10); r.raise_for_status()
        jd = r.json()
        if jd.get('code') == '200000':
            data = jd['data']
            return data['token'], data['instanceServers'][0]['endpoint']
    except requests.exceptions.RequestException as e:
        print("Lỗi token:", e)
    return None, None

def _prepare_row(msg):
    sym = msg['topic'].split(':')[-1]
    d   = msg['data']; ts = d['timestamp']
    bids, asks = d.get('bids', []), d.get('asks', [])
    row = [ts, sym]
    for i in range(LIMIT):
        bp, bv = (bids[i][0], bids[i][1]) if i < len(bids) else ('', '')
        ap, av = (asks[i][0], asks[i][1]) if i < len(asks) else ('', '')
        row += [bp, ap, bv, av]
    return sym, row

def process_batch(batch):
    by_file = {}
    for msg in batch:
        sym, row = _prepare_row(msg)
        fn = f'/Users/hieuduc/Downloads/hft_live/data/{sym.upper()}_kucoin_orderbook.csv'
        by_file.setdefault(fn, []).append(row)

    for fn, rows in by_file.items():
        # TẠO THƯ MỤC ĐÍCH NẾU CHƯA CÓ
        dirpath = os.path.dirname(fn)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)

        # Khởi tạo file + header nếu file chưa tồn tại hoặc rỗng
        if fn not in initialized_files:
            if not os.path.exists(fn) or os.path.getsize(fn) == 0:
                with open(fn, 'w', newline='', encoding='utf-8') as f:
                    csv.writer(f).writerow(header)
            initialized_files.add(fn)

        # Append data
        with open(fn, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(rows)

    # print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Ghi {len(batch)} dòng.")

# ───── CORE WEBSOCKET CLIENT ─────────────────────────────────
async def kucoin_ws_client():
    buffer = []
    while True:
        try:
            token, endpoint = get_kucoin_ws_token()
            if not token:
                await asyncio.sleep(10)
                continue

            cid = str(int(time.time()*1000))
            url = f"{endpoint}?token={token}&connectId={cid}"

            async with websockets.connect(url, ping_interval=18) as ws:
                # print("Đã kết nối WebSocket KuCoin.")
                for sym in SYMBOLS:
                    topic = f"/contractMarket/level2Depth{LIMIT}:{sym}"
                    await ws.send(json.dumps({
                        "id": f"{cid}_{sym}", "type": "subscribe",
                        "topic": topic, "response": True
                    }))
                    # print("Sub", topic)

                async for msg in ws:
                    jd = json.loads(msg)
                    if jd.get('type') == 'ping':
                        await ws.send(json.dumps({"id": jd['id'], "type": "pong"}))
                        continue
                    if jd.get('type') == 'message':
                        buffer.append(jd)
                        if len(buffer) >= BATCH_SIZE:
                            process_batch(buffer)
                            buffer.clear()

        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.ConnectionClosedError) as e:
            # print("Mất kết nối:", e)
            if buffer:
                process_batch(buffer)
                buffer.clear()
            await asyncio.sleep(5)
        except Exception as e:
            # print("Lỗi khác:", e)
            if buffer:
                process_batch(buffer)
                buffer.clear()
            await asyncio.sleep(10)

async def main():
    await kucoin_ws_client()

if __name__ == "__main__":
    asyncio.run(main())
