# coding: utf-8
import asyncio
import websockets
import json
import time
import requests
import threading
import queue
import math
import csv
import os
import hmac
import base64
import hashlib
import urllib.parse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ===============================================
# 1) CẤU HÌNH CHUNG
# ===============================================

RAW_SYMBOLS = ["ENAUSDT"]


def kucoin_futures_code(spot: str) -> str:
    """BTCUSDT -> BTCUSDTM (perpetual)."""
    return spot.replace("USDT", "USDTM")


SYMBOLS = [kucoin_futures_code(s) for s in RAW_SYMBOLS]

RAW_QUEUE_MAXSIZE = 50000   # tick WS - tăng để xử lý burst
TICK_QUEUE_MAXSIZE = 50000  # tick đã parse - tăng để xử lý burst
CSV_QUEUE_MAXSIZE = 1000    # queue cho CSV writing

TICK_SIZE = 0.0001
MAX_GRID_LEVELS = 128

# Thư mục & file CSV lưu quote rounds
CSV_DIR = "./quotes_logs"
os.makedirs(CSV_DIR, exist_ok=True)

# ===============================================
# CPU CONFIGURATION
# ===============================================
CPU_COUNT = os.cpu_count() or 4  # Số CPU cores, default 4 nếu không detect được
PARSER_WORKERS = max(1, CPU_COUNT - 2)  # Số parser workers (để lại cores cho WS, Trading, Alpha, CSV)
print(f">>> Detected {CPU_COUNT} CPU cores, using {PARSER_WORKERS} parser workers")

# ===============================================
# API CREDENTIALS CHO TRADING
# ===============================================
# Lấy từ biến môi trường để bảo mật
API_KEY = os.getenv("KUCOIN_API_KEY")
API_SECRET = os.getenv("KUCOIN_API_SECRET")
API_PASSPHRASE = os.getenv("KUCOIN_API_PASSPHRASE")

if not API_KEY or not API_SECRET or not API_PASSPHRASE:
    raise RuntimeError(
        "Vui lòng set các biến môi trường:\n"
        "  export KUCOIN_API_KEY='your_api_key'\n"
        "  export KUCOIN_API_SECRET='your_api_secret'\n"
        "  export KUCOIN_API_PASSPHRASE='your_passphrase'\n"
        "Hoặc tạo file .env và load bằng python-dotenv"
    )

# Margin mode cho futures orders: "ISOLATED" hoặc "CROSSED"
MARGIN_MODE = os.getenv("KUCOIN_MARGIN_MODE", "ISOLATED")


# ===============================================
# 2) WEBSOCKET KUCOIN FUTURES
# ===============================================

def get_kucoin_ws_token():
    """Lấy public-token & endpoint cho WebSocket Futures."""
    url = "https://api-futures.kucoin.com/api/v1/bullet-public"
    try:
        r = requests.post(url, timeout=10)
        r.raise_for_status()
        jd = r.json()
        if jd.get("code") == "200000":
            data = jd["data"]
            return data["token"], data["instanceServers"][0]["endpoint"]
    except requests.exceptions.RequestException as e:
        print("Lỗi token:", e)
    return None, None


async def kucoin_ws_client(symbols, raw_q: queue.Queue, stop_event: threading.Event):
    """Client WebSocket chạy trong thread riêng, đẩy raw message vào RAW_QUEUE."""
    while not stop_event.is_set():
        token, endpoint = get_kucoin_ws_token()
        if not token:
            await asyncio.sleep(10.0)
            continue

        cid = str(int(time.time() * 1000))
        url = f"{endpoint}?token={token}&connectId={cid}"

        try:
            async with websockets.connect(url, ping_interval=18) as ws:
                # subscribe
                for sym in symbols:
                    topic = f"/contractMarket/level2Depth50:{sym}"
                    sub_msg = {
                        "id": f"{cid}_{sym}",
                        "type": "subscribe",
                        "topic": topic,
                        "response": True,
                    }
                    await ws.send(json.dumps(sub_msg))

                while not stop_event.is_set():
                    msg = await ws.recv()
                    jd = json.loads(msg)
                    t = jd.get("type")

                    if t == "ping":
                        await ws.send(json.dumps({"id": jd.get("id", cid), "type": "pong"}))
                        continue

                    if t == "message":
                        try:
                            raw_q.put_nowait(jd)
                        except queue.Full:
                            # queue đầy thì bỏ bớt tick cũ để ưu tiên data mới
                            try:
                                raw_q.get_nowait()  # Bỏ 1 message cũ
                                raw_q.put_nowait(jd)  # Thêm message mới
                            except queue.Empty:
                                pass

        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
            print("Mất kết nối WS, reconnect...", e)
            await asyncio.sleep(5.0)
        except Exception as e:
            print("Lỗi WS khác:", e)
            await asyncio.sleep(10.0)


def ws_thread_fn(symbols, raw_q: queue.Queue, stop_event: threading.Event):
    asyncio.run(kucoin_ws_client(symbols, raw_q, stop_event))


# ===============================================
# 3) PARSER TỪ RAW WS → TICK ĐƠN GIẢN
# ===============================================

def parse_loop(raw_q: queue.Queue, tick_q: queue.Queue, stop_event: threading.Event):
    """
    Đọc raw message từ RAW_QUEUE, parse thành:
        {timestamp, symbol, best_bid, best_ask}
    rồi đẩy vào TICK_QUEUE.
    Tối ưu: batch processing để xử lý nhiều messages cùng lúc.
    """
    batch = []
    batch_size = 100
    last_batch_time = time.time()
    
    while not stop_event.is_set():
        try:
            # Lấy message với timeout ngắn hơn
            msg = raw_q.get(timeout=0.1)
            if msg is None:
                # Flush batch trước khi break
                if batch:
                    for tick in batch:
                        try:
                            tick_q.put_nowait(tick)
                        except queue.Full:
                            pass
                break
            
            batch.append(msg)
            
            # Xử lý batch khi đủ size hoặc timeout
            now = time.time()
            if len(batch) >= batch_size or (now - last_batch_time) > 0.01:
                # Parse batch
                ticks_to_add = []
                for msg_item in batch:
                    topic = msg_item.get("topic", "")
                    if not topic:
                        continue
                    
                    try:
                        symbol = topic.split(":")[-1]
                    except Exception:
                        continue
                    
                    data = msg_item.get("data", {})
                    ts_ms = data.get("timestamp")
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    
                    if ts_ms is None or not bids or not asks:
                        continue
                    
                    try:
                        ts = datetime.utcfromtimestamp(ts_ms / 1000.0)
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                    except Exception:
                        continue
                    
                    tick = {
                        "timestamp": ts,
                        "symbol": symbol,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                    }
                    ticks_to_add.append(tick)
                
                # Đẩy batch vào queue
                for tick in ticks_to_add:
                    try:
                        tick_q.put_nowait(tick)
                    except queue.Full:
                        # Queue đầy thì bỏ bớt tick cũ
                        try:
                            tick_q.get_nowait()  # Bỏ 1 tick cũ
                            tick_q.put_nowait(tick)  # Thêm tick mới
                        except queue.Empty:
                            pass
                
                batch = []
                last_batch_time = now
                
        except queue.Empty:
            # Flush batch nếu có và đã timeout
            if batch and (time.time() - last_batch_time) > 0.05:
                ticks_to_add = []
                for msg_item in batch:
                    topic = msg_item.get("topic", "")
                    if not topic:
                        continue
                    try:
                        symbol = topic.split(":")[-1]
                    except Exception:
                        continue
                    data = msg_item.get("data", {})
                    ts_ms = data.get("timestamp")
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    if ts_ms is None or not bids or not asks:
                        continue
                    try:
                        ts = datetime.utcfromtimestamp(ts_ms / 1000.0)
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                    except Exception:
                        continue
                    tick = {
                        "timestamp": ts,
                        "symbol": symbol,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                    }
                    ticks_to_add.append(tick)
                
                for tick in ticks_to_add:
                    try:
                        tick_q.put_nowait(tick)
                    except queue.Full:
                        try:
                            tick_q.get_nowait()
                            tick_q.put_nowait(tick)
                        except queue.Empty:
                            pass
                
                batch = []
                last_batch_time = time.time()
            continue


# ===============================================
# 4) LIVE GRID ENGINE STATEFUL (KHÔNG DÙNG NUMBA)
# ===============================================

def snap_down(price: float, tick_size: float) -> float:
    if price <= 0.0:
        return 0.0
    ticks = math.floor(price / tick_size + 1e-12)
    return ticks * tick_size


def snap_up(price: float, tick_size: float) -> float:
    if price <= 0.0:
        return 0.0
    ticks = math.ceil(price / tick_size - 1e-12)
    return ticks * tick_size


class LiveGridEngine:
    """
    Bản live của chiến lược grid MM:
    - Giữ trạng thái inventory, equity, fee, quote grid.
    - Mỗi tick: cập nhật grid nếu mid di chuyển đủ; mô phỏng fill dựa trên best_bid/best_ask.
    - Quantity luôn là số nguyên trong [1, 3] ENA.
    """

    def __init__(
        self,
        fee: float,
        max_position: float,
        half_spread_ticks: int,
        price_range_ticks: int,
        grid_num: int,
        update_threshold_ticks: int,
        dollar_qty: float = 10.0,
    ):
        self.fee = float(fee)
        self.max_position = float(max_position)
        self.half_spread = half_spread_ticks * TICK_SIZE
        self.price_range = price_range_ticks * TICK_SIZE
        self.grid_num = int(grid_num)
        self.update_threshold_price = update_threshold_ticks * TICK_SIZE

        self.dollar_qty = float(dollar_qty)

        # size trong khoảng [1, 3] ENA
        self.min_size = 1.0
        self.max_size = 1.0

        # state
        self.running_qty = 0.0
        self.static_equity = 0.0
        self.fee_paid = 0.0
        self.fills = 0

        self.prev_mid = None
        self.initialized = False

        self.current_nb = 0
        self.current_na = 0
        self.bid_prices = [0.0] * MAX_GRID_LEVELS
        self.ask_prices = [0.0] * MAX_GRID_LEVELS

        self.soft_cap = 0.6
        self.k_skew = 3.0  # độ mạnh inventory skew

    def _compute_int_size(self, price: float) -> float:
        """
        Tính quantity integer dựa trên dollar_qty
        và ép trong đoạn [min_size, max_size], ở đây là [1, 3] ENA.
        """
        if price <= 0.0:
            return 0.0

        raw = self.dollar_qty / price          # số ENA theo đô
        size_int = int(raw)                    # floor

        if size_int < int(self.min_size):
            size_int = int(self.min_size)
        if size_int > int(self.max_size):
            size_int = int(self.max_size)

        return float(size_int)

    def _rebuild_grid(self, mid: float, best_bid: float, best_ask: float, vol_factor: float):
        # clamp vol_factor
        vf = vol_factor
        if not math.isfinite(vf) or vf <= 0.0:
            vf = 1.0
        if vf < 0.25:
            vf = 0.25
        if vf > 4.0:
            vf = 4.0

        half_spread_local = self.half_spread * vf
        price_range_local = self.price_range * vf
        max_position_local = self.max_position / vf if self.max_position != 0.0 else self.max_position

        # tỷ lệ inventory
        pos_value = self.running_qty * mid
        denom = max_position_local if max_position_local != 0.0 else 1.0
        x = pos_value / denom  # ~ [-inf, +inf]

        # xác định số levels mỗi phía
        nb = self.grid_num
        na = self.grid_num

        # hard cap
        if x >= 1.0:
            nb = 0  # không mua thêm
        elif x <= -1.0:
            na = 0  # không bán thêm
        else:
            # soft cap giảm dần levels theo |x|
            if x > self.soft_cap:
                ratio = (1.0 - x) / (1.0 - self.soft_cap)
                if ratio < 0.0:
                    ratio = 0.0
                nb = int(self.grid_num * ratio)
            elif x < -self.soft_cap:
                ratio = (1.0 + x) / (1.0 - self.soft_cap)
                if ratio < 0.0:
                    ratio = 0.0
                na = int(self.grid_num * ratio)

        if nb < 0:
            nb = 0
        if na < 0:
            na = 0

        self.current_nb = nb
        self.current_na = na

        # inventory skew exp
        scale_bid = math.exp(self.k_skew * x)
        scale_ask = math.exp(-self.k_skew * x)

        # clamp scale
        scale_bid = min(max(scale_bid, 0.05), 20.0)
        scale_ask = min(max(scale_ask, 0.05), 20.0)

        half_spread_bid = half_spread_local * scale_bid
        half_spread_ask = half_spread_local * scale_ask

        # step giữa các levels
        if self.grid_num > 1:
            step = price_range_local / (self.grid_num - 1)
            if step < 0.0:
                step = 0.0
        else:
            step = 0.0

        # build BID grid
        for j in range(self.current_nb):
            offset = half_spread_bid + j * step
            raw_price = mid - offset
            if raw_price > best_bid:
                raw_price = best_bid
            bp = snap_down(raw_price, TICK_SIZE)
            if bp <= 0.0:
                bp = 0.0
            self.bid_prices[j] = bp
        for j in range(self.current_nb, MAX_GRID_LEVELS):
            self.bid_prices[j] = 0.0

        # build ASK grid
        for j in range(self.current_na):
            offset = half_spread_ask + j * step
            raw_price = mid + offset
            if raw_price < best_ask:
                raw_price = best_ask
            ap = snap_up(raw_price, TICK_SIZE)
            if ap <= 0.0:
                ap = 0.0
            self.ask_prices[j] = ap
        for j in range(self.current_na, MAX_GRID_LEVELS):
            self.ask_prices[j] = 0.0

        self.prev_mid = mid
        self.initialized = True

    def step(self, best_bid: float, best_ask: float, vol_factor: float):
        """
        Một tick:
        - Cập nhật grid nếu mid di chuyển đủ;
        - Mô phỏng fill dựa trên best_bid/best_ask;
        - Trả về toàn bộ thông tin grid + inventory + equity.
        Quantity luôn là số nguyên trong [1, 3] ENA.
        """
        if best_bid <= 0.0 or best_ask <= 0.0:
            return None

        mid = 0.5 * (best_bid + best_ask)

        # quyết định có rebuild grid hay không
        need_update = False
        if (not self.initialized) or self.update_threshold_price <= 0.0:
            need_update = True
        else:
            if self.prev_mid is None:
                need_update = True
            else:
                if abs(mid - self.prev_mid) >= self.update_threshold_price:
                    need_update = True

        if need_update:
            self._rebuild_grid(mid, best_bid, best_ask, vol_factor)

        # mô phỏng fill: coi tick này high=best_ask, low=best_bid
        high = best_ask
        low = best_bid

        # BID fills
        if self.current_nb > 0:
            for j in range(self.current_nb):
                bp = self.bid_prices[j]
                if bp <= 0.0:
                    continue
                # nếu low <= price của mình → đã được fill
                if low <= bp:
                    order_qty = self._compute_int_size(bp)
                    if order_qty <= 0.0:
                        continue
                    self.running_qty += order_qty
                    self.static_equity -= bp * order_qty
                    self.fee_paid += bp * order_qty * self.fee
                    self.fills += 1

        # ASK fills
        if self.current_na > 0:
            for j in range(self.current_na):
                ap = self.ask_prices[j]
                if ap <= 0.0:
                    continue
                # nếu high >= price của mình → đã được fill
                if high >= ap:
                    order_qty = self._compute_int_size(ap)
                    if order_qty <= 0.0:
                        continue
                    self.running_qty -= order_qty
                    self.static_equity += ap * order_qty
                    self.fee_paid += ap * order_qty * self.fee
                    self.fills += 1

        equity = self.static_equity + self.running_qty * mid - self.fee_paid

        # Chuẩn bị list quote + quantity cho từng level (integer size)
        bid_quotes = []
        for j in range(self.current_nb):
            p = self.bid_prices[j]
            if p > 0.0:
                q = self._compute_int_size(p)
                if q <= 0.0:
                    continue
                bid_quotes.append((j, p, q))

        ask_quotes = []
        for j in range(self.current_na):
            p = self.ask_prices[j]
            if p > 0.0:
                q = self._compute_int_size(p)
                if q <= 0.0:
                    continue
                ask_quotes.append((j, p, q))

        return {
            "mid": mid,
            "vol_factor": vol_factor,
            "running_qty": self.running_qty,
            "equity": equity,
            "fills": self.fills,
            "nb": self.current_nb,
            "na": self.current_na,
            "bid_quotes": bid_quotes,   # [(level, price, qty), ...]
            "ask_quotes": ask_quotes,   # [(level, price, qty), ...]
        }


# ===============================================
# 5) TRADING WEBSOCKET FUNCTIONS
# ===============================================

def build_ws_trading_url() -> str:
    """Tạo URL WebSocket trading."""
    ts_ms = str(int(time.time() * 1000))
    
    # sign = HMAC(secret, apikey + timestamp)
    prehash = API_KEY + ts_ms
    sign_bytes = hmac.new(
        API_SECRET.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    sign_b64 = base64.b64encode(sign_bytes).decode("utf-8")
    
    # passphrase mã hoá bằng secret
    passphrase_bytes = hmac.new(
        API_SECRET.encode("utf-8"),
        API_PASSPHRASE.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    passphrase_b64 = base64.b64encode(passphrase_bytes).decode("utf-8")
    
    query = urllib.parse.urlencode(
        {
            "apikey": API_KEY,
            "sign": sign_b64,
            "passphrase": passphrase_b64,
            "timestamp": ts_ms,
        }
    )
    
    return f"wss://wsapi.kucoin.com/v1/private?{query}"


async def authenticate_session(ws) -> None:
    """Authenticate WebSocket trading session."""
    challenge_raw = await ws.recv()
    
    try:
        challenge = json.loads(challenge_raw)
        if "code" in challenge and challenge.get("code") != "200000":
            raise RuntimeError(f"Server trả lỗi ngay khi connect: {challenge}")
    except json.JSONDecodeError:
        pass
    
    sig_bytes = hmac.new(
        API_SECRET.encode("utf-8"),
        challenge_raw.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    sig_b64 = base64.b64encode(sig_bytes).decode("utf-8")
    
    await ws.send(sig_b64)
    
    welcome_raw = await ws.recv()
    welcome = json.loads(welcome_raw)
    if welcome.get("data") != "welcome":
        raise RuntimeError(f"Session verification FAILED: {welcome}")


async def cancel_futures_order_by_order_id(ws, symbol: str, order_id: str) -> dict:
    """Cancel một order bằng orderId."""
    req_id = f"cancel_{order_id}"[-32:]
    
    msg = {
        "id": req_id,
        "op": "futures.cancel",
        "args": {
            "symbol": symbol,
            "orderId": order_id,
        },
    }
    
    await ws.send(json.dumps(msg))
    
    raw = await ws.recv()
    data = json.loads(raw)
    
    result = {
        "success": False,
        "symbol": symbol,
        "orderId": order_id,
        "cancelledOrderIds": [],
        "code": data.get("code"),
        "msg": data.get("msg"),
    }
    
    if data.get("op") != "futures.cancel" or data.get("id") != req_id:
        return result
    
    if data.get("code") != "200000":
        return result
    
    cancelled_ids = data.get("data", {}).get("cancelledOrderIds") or []
    result["success"] = True
    result["cancelledOrderIds"] = cancelled_ids
    return result


async def place_futures_order(ws, symbol: str, side: str, price: float, size: int) -> dict:
    """Đặt lệnh futures limit order - giống test_order.py."""
    now_ms = int(time.time() * 1000)
    
    # Round price về đúng tick size để tránh lỗi "Price parameter invalid"
    # TICK_SIZE = 0.0001, cần round về bội số của 0.0001
    price_rounded = round(price / TICK_SIZE) * TICK_SIZE
    # Format price với đúng số chữ số thập phân (4 chữ số cho tick size 0.0001)
    price_str_formatted = f"{price_rounded:.4f}"
    
    # clientOid chỉ dùng chữ/số/_/-, rút ngắn để tránh lỗi "Field 'id' too long"
    price_str = price_str_formatted.replace('.', '')
    # Chỉ lấy tối đa 10 ký tự từ price để tránh quá dài
    if len(price_str) > 10:
        price_str = price_str[:10]
    
    symbol_short = symbol[:3].lower() if len(symbol) >= 3 else symbol.lower()
    side_short = side[:1]  # b hoặc s thay vì buy/sell
    # Format: ena_b_2304_1234567890 (ngắn gọn)
    client_oid = f"{symbol_short}_{side_short}_{price_str}_{now_ms}"
    
    # ONE-WAY mode: không cần positionSide field
    # Chỉ cần side (buy/sell) là đủ
    
    msg = {
        "id": client_oid,
        "op": "futures.order",
        "args": {
            "clientOid": client_oid,
            "symbol": symbol,
            "marginMode": MARGIN_MODE,
            "leverage": 1,
            # Bỏ positionSide - ONE-WAY mode không cần field này
            "side": side,
            "type": "limit",
            "size": size,
            "price": price_str_formatted,  # Dùng price đã rounded
            "timeInForce": "GTC",
            "reduceOnly": False,
            "timestamp": now_ms,
        }
    }
    
    await ws.send(json.dumps(msg))
    
    raw = await ws.recv()
    data = json.loads(raw)
    
    # Mặc định: fail
    result = {
        "success": False,
        "symbol": symbol,
        "side": side,
        "price": price,
        "quantity": size,
        "orderId": None,
        "clientOid": client_oid,
        "code": data.get("code"),
        "msg": data.get("msg"),
    }
    
    # Check đúng response futures.order
    if data.get("op") != "futures.order":
        # Có thể là response của request khác, log để debug
        print(f">>> Warning: Received unexpected op: {data.get('op')}, expected: futures.order")
        return result  # sai format thì coi như thất bại
    
    # Check id match (có thể bỏ qua nếu response không có id hoặc id khác)
    response_id = data.get("id")
    if response_id and response_id != client_oid:
        print(f">>> Warning: Response id mismatch: got {response_id}, expected {client_oid}")
        # Vẫn tiếp tục xử lý vì có thể là response của request trước đó bị delay
    
    code = data.get("code")
    if code != "200000":
        # KuCoin trả lỗi, không có orderId
        result["msg"] = data.get("msg")
        return result
    
    # Thành công - code = 200000
    order_id = data.get("data", {}).get("orderId")
    if order_id:
        result["success"] = True
        result["orderId"] = order_id
    else:
        # Code 200000 nhưng không có orderId - có thể là response của request khác
        print(f">>> Warning: Code 200000 but no orderId in response. Response: {data}")
        result["msg"] = "No orderId in response"
    
    return result


# ===============================================
# 6) TRADING MANAGER (ASYNC)
# ===============================================

async def trading_manager_loop(
    trading_q: queue.Queue,
    stop_event: threading.Event
):
    """
    Async loop quản lý WebSocket trading:
    - Nhận commands từ queue: {"action": "cancel", "order_ids": [...]} hoặc {"action": "place", "orders": [...]}
    - Trả kết quả về queue: {"action": "cancel_result", ...} hoặc {"action": "place_result", ...}
    - WebSocket không hỗ trợ concurrent recv(), nên cần chạy tuần tự
    """
    ws = None
    
    while not stop_event.is_set():
        try:
            # Lấy command từ queue
            try:
                cmd = trading_q.get(timeout=1.0)
            except queue.Empty:
                continue
            
            if cmd is None:
                break
            
            # Nếu chưa có connection, tạo mới
            if ws is None:
                try:
                    ws_url = build_ws_trading_url()
                    ws = await websockets.connect(ws_url)
                    await authenticate_session(ws)
                    print(">>> Trading WebSocket connected and authenticated")
                except Exception as e:
                    print(f">>> Trading WS connection error: {e}")
                    ws = None
                    await asyncio.sleep(5.0)
                    continue
            
            # Xử lý command
            action = cmd.get("action")
            
            if action == "cancel":
                # Cancel orders - chạy tuần tự (websocket không hỗ trợ concurrent recv)
                # QUAN TRỌNG: Phải cancel thành công để tránh tích lũy orders
                symbol = cmd.get("symbol")
                order_ids = cmd.get("order_ids", [])
                cancelled_results = []
                
                print(f">>> Cancelling {len(order_ids)} orders...")
                
                for order_id in order_ids:
                    try:
                        result = await cancel_futures_order_by_order_id(ws, symbol, order_id)
                        cancelled_results.append(result)
                        if not result.get("success"):
                            print(f">>> Cancel failed for {order_id}: {result.get('msg')}")
                    except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
                        print(f">>> Cancel error (connection closed) for {order_id}: {e}")
                        if ws:
                            try:
                                await ws.close()
                            except:
                                pass
                        ws = None
                        cancelled_results.append({
                            "success": False,
                            "orderId": order_id,
                            "error": str(e)
                        })
                        # Nếu connection bị đóng, cần reconnect trước khi cancel tiếp
                        # Break để reconnect ở vòng lặp tiếp theo, các orders còn lại sẽ được cancel sau
                        break
                    except Exception as e:
                        print(f">>> Cancel error for {order_id}: {e}")
                        cancelled_results.append({
                            "success": False,
                            "orderId": order_id,
                            "error": str(e)
                        })
                
                # Đếm số orders cancel thành công
                success_count = sum(1 for r in cancelled_results if r.get("success"))
                print(f">>> Cancelled {success_count}/{len(order_ids)} orders successfully")
                
                # Trả kết quả
                result_q = cmd.get("result_queue")
                if result_q:
                    result_q.put({
                        "action": "cancel_result",
                        "results": cancelled_results
                    })
            
            elif action == "place":
                # Place orders - chạy tuần tự (websocket không hỗ trợ concurrent recv)
                orders = cmd.get("orders", [])  # [{"symbol": ..., "side": ..., "price": ..., "size": ...}, ...]
                placed_results = []
                
                print(f">>> Placing {len(orders)} orders...")
                for order in orders:
                    try:
                        result = await place_futures_order(
                            ws,
                            order["symbol"],
                            order["side"],
                            order["price"],
                            order["size"]
                        )
                        if result.get("success"):
                            print(f">>> Order placed: {order['side']} {order['symbol']} @ {order['price']} qty={order['size']} orderId={result.get('orderId')}")
                        else:
                            print(f">>> Order failed: {order['side']} {order['symbol']} @ {order['price']} code={result.get('code')} msg={result.get('msg')}")
                        placed_results.append(result)
                    except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError) as e:
                        print(f">>> Place order error (connection closed): {e}")
                        if ws:
                            try:
                                await ws.close()
                            except:
                                pass
                        ws = None
                        placed_results.append({
                            "success": False,
                            "symbol": order.get("symbol"),
                            "side": order.get("side"),
                            "price": order.get("price"),
                            "error": str(e)
                        })
                        break  # Stop processing, will reconnect on next iteration
                    except Exception as e:
                        print(f">>> Place order error: {e}")
                        placed_results.append({
                            "success": False,
                            "symbol": order.get("symbol"),
                            "side": order.get("side"),
                            "price": order.get("price"),
                            "error": str(e)
                        })
                
                # Trả kết quả
                result_q = cmd.get("result_queue")
                if result_q:
                    result_q.put({
                        "action": "place_result",
                        "results": placed_results
                    })
            
        except Exception as e:
            print(f">>> Trading manager error: {e}")
            if ws:
                try:
                    await ws.close()
                except:
                    pass
                ws = None
            await asyncio.sleep(5.0)
    
    # Cleanup
    if ws:
        try:
            await ws.close()
        except:
            pass


def trading_manager_thread_fn(trading_q: queue.Queue, stop_event: threading.Event):
    """Thread wrapper cho trading manager."""
    asyncio.run(trading_manager_loop(trading_q, stop_event))


# ===============================================
# 6) CSV WRITER THREAD (TÁCH RA ĐỂ KHÔNG BLOCK ALPHA LOOP)
# ===============================================

def csv_writer_loop(csv_q: queue.Queue, stop_event: threading.Event):
    """
    Thread riêng để ghi CSV, không block alpha_loop.
    Nhận data từ queue và ghi vào file.
    """
    file_handles = {}  # {symbol: file_handle}
    file_writers = {}  # {symbol: csv_writer}
    file_headers_written = {}  # {symbol: bool}
    
    while not stop_event.is_set():
        try:
            data = csv_q.get(timeout=1.0)
            if data is None:
                break
            
            symbol = data.get("symbol")
            csv_path = data.get("csv_path")
            header = data.get("header")
            rows = data.get("rows", [])
            
            if not symbol or not csv_path:
                continue
            
            # Mở file nếu chưa mở
            if symbol not in file_handles:
                f = open(csv_path, "a", newline="", encoding="utf-8")
                writer = csv.writer(f)
                file_handles[symbol] = f
                file_writers[symbol] = writer
                file_headers_written[symbol] = False
            
            writer = file_writers[symbol]
            
            # Ghi header nếu cần
            if header and not file_headers_written[symbol]:
                # Check file empty
                try:
                    if os.path.getsize(csv_path) == 0:
                        writer.writerow(header)
                        file_headers_written[symbol] = True
                except OSError:
                    # File không tồn tại hoặc lỗi, ghi header
                    writer.writerow(header)
                    file_headers_written[symbol] = True
            
            # Ghi rows
            if rows:
                writer.writerows(rows)
                file_handles[symbol].flush()  # Flush ngay để đảm bảo data được ghi
                
        except queue.Empty:
            continue
        except Exception as e:
            print(f">>> CSV writer error: {e}")
    
    # Close all files
    for symbol, f in file_handles.items():
        try:
            f.close()
        except:
            pass


# ===============================================
# 7) ALPHA LOOP: TỪ TICK → VOL_FACTOR → GRID ENGINE
#    + ROUND MỚI CHỈ KHI GRID THAY ĐỔI & GHI CSV THEO ROUND
#    + CANCEL ORDERS CŨ & PLACE ORDERS MỚI
# ===============================================

def alpha_loop(tick_q: queue.Queue, trading_q: queue.Queue, csv_q: queue.Queue, stop_event: threading.Event):
    """
    - Nhận tick từng ms, tính vol_factor EWMA.
    - Feed vào LiveGridEngine để cập nhật inventory/equity/fill liên tục.
    - CHỈ KHI GRID (các mức giá/qty BID+ASK) THAY ĐỔI so với round trước
      → tạo ROUND MỚI:
        * round_id += 1
        * cancel orders của round trước
        * place orders mới
        * log console
        * ghi toàn bộ grid (mỗi level 1 dòng) vào CSV với order_id.
    """

    engine = LiveGridEngine(
        fee=0.0002,
        max_position=100.0,
        half_spread_ticks=3,
        price_range_ticks=105,
        grid_num=5,
        update_threshold_ticks=1,
        dollar_qty=10.0,
    )

    # vol EWMA online
    last_mid_for_vol = None
    sigma2 = None
    baseline_sigma = None
    lambda_ = 0.94
    clip_min = 0.5
    clip_max = 2.0

    # round & CSV
    round_id = 0
    last_quote_mid = None
    last_grid_sig = None  # signature của grid vòng trước
    last_round_order_ids = []  # lưu order_ids của round trước để cancel

    print("Alpha loop started (tick-by-tick, quote-on-GRID-change).")

    # Tối ưu: xử lý tick với timeout ngắn để không block lâu
    while not stop_event.is_set():
        try:
            tick = tick_q.get(timeout=0.01)  # Timeout ngắn hơn để responsive
        except queue.Empty:
            continue

        if tick is None:
            break

        ts = tick["timestamp"]
        symbol = tick["symbol"]
        bb = tick["best_bid"]
        ba = tick["best_ask"]
        mid_raw = 0.5 * (bb + ba)

        # vol_factor từ log-return trên mid tick (dùng last_mid_for_vol)
        if last_mid_for_vol is None or mid_raw <= 0.0 or last_mid_for_vol <= 0.0:
            vol_factor = 1.0
        else:
            r = math.log(mid_raw / last_mid_for_vol)
            if sigma2 is None:
                sigma2 = r * r
            else:
                sigma2 = lambda_ * sigma2 + (1.0 - lambda_) * r * r

            sigma = math.sqrt(sigma2) if sigma2 is not None and sigma2 > 0 else 1e-8

            if baseline_sigma is None or not math.isfinite(baseline_sigma) or baseline_sigma <= 0.0:
                baseline_sigma = sigma
            else:
                baseline_sigma = 0.99 * baseline_sigma + 0.01 * sigma

            if baseline_sigma <= 0.0:
                baseline_sigma = 1e-4

            vol_factor = sigma / baseline_sigma
            if vol_factor < clip_min:
                vol_factor = clip_min
            elif vol_factor > clip_max:
                vol_factor = clip_max

        last_mid_for_vol = mid_raw

        # Update engine mỗi tick (fill + inventory + equity)
        res = engine.step(best_bid=bb, best_ask=ba, vol_factor=vol_factor)
        if res is None:
            continue

        mid_ = res["mid"]
        vf_ = res["vol_factor"]
        qty_ = res["running_qty"]
        eq_ = res["equity"]
        fills_ = res["fills"]
        bid_quotes = res["bid_quotes"]
        ask_quotes = res["ask_quotes"]
        num_quotes = len(bid_quotes) + len(ask_quotes)

        # Tạo signature cho grid hiện tại: (side, level, price, qty_int)
        grid_sig = tuple(
            [("B", lvl, round(price, 8), int(qty)) for (lvl, price, qty) in bid_quotes] +
            [("A", lvl, round(price, 8), int(qty)) for (lvl, price, qty) in ask_quotes]
        )

        # Chỉ khi GRID (các mức BID/ASK + qty) THAY ĐỔI mới tạo round mới
        if last_grid_sig is not None and grid_sig == last_grid_sig:
            # Lưới y hệt round trước → bỏ qua, không dump
            continue

        # GRID khác → round mới
        round_id += 1
        last_quote_mid = mid_
        last_grid_sig = grid_sig

        # ====== CANCEL ORDERS CỦA ROUND TRƯỚC ======
        # QUAN TRỌNG: Phải đảm bảo cancel thành công trước khi place orders mới
        cancel_success_count = 0
        if last_round_order_ids and round_id > 1:
            result_queue = queue.Queue()
            cancel_cmd = {
                "action": "cancel",
                "symbol": symbol,
                "order_ids": last_round_order_ids,
                "result_queue": result_queue,
            }
            try:
                trading_q.put_nowait(cancel_cmd)
                # Đợi kết quả với timeout dài hơn để đảm bảo cancel hoàn tất
                try:
                    cancel_result = result_queue.get(timeout=15.0)
                    if cancel_result.get("action") == "cancel_result":
                        results = cancel_result.get("results", [])
                        cancelled = [r for r in results if r.get("success")]
                        failed = [r for r in results if not r.get("success")]
                        cancel_success_count = len(cancelled)
                        if cancelled:
                            print(f">>> Cancelled {len(cancelled)}/{len(last_round_order_ids)} orders")
                        if failed:
                            print(f">>> WARNING: {len(failed)} orders failed to cancel: {[r.get('orderId') for r in failed[:3]]}")
                            # Nếu có nhiều orders fail, có thể là vấn đề nghiêm trọng
                            if len(failed) > len(cancelled):
                                print(f">>> ERROR: More orders failed to cancel than succeeded! May have orphaned orders.")
                except queue.Empty:
                    print(f">>> WARNING: Timeout waiting for cancel results. Orders may not be cancelled.")
                    # Timeout - không chắc orders đã được cancel, nhưng vẫn tiếp tục
                    pass
            except queue.Full:
                # Queue đầy thì bỏ bớt command cũ
                try:
                    trading_q.get_nowait()
                    trading_q.put_nowait(cancel_cmd)
                except queue.Empty:
                    pass
        
        # ====== PLACE ORDERS MỚI ======
        # CHỈ PLACE ORDERS MỚI SAU KHI ĐÃ CANCEL THÀNH CÔNG (hoặc không có orders cũ)
        new_order_ids = []
        place_results = []  # Lưu toàn bộ results để map chính xác
        
        # Nếu có orders cũ cần cancel nhưng cancel fail nhiều, cảnh báo
        should_place_new_orders = True
        if last_round_order_ids and round_id > 1:
            expected_cancelled = len(last_round_order_ids)
            if cancel_success_count < expected_cancelled * 0.5:  # Nếu cancel < 50% thành công
                print(f">>> WARNING: Only {cancel_success_count}/{expected_cancelled} orders cancelled. "
                      f"May have {expected_cancelled - cancel_success_count} orphaned orders.")
                # Nếu cancel fail quá nhiều (>50%), có thể có vấn đề nghiêm trọng
                # Vẫn place orders mới nhưng log warning
                if cancel_success_count == 0:
                    print(f">>> ERROR: All {expected_cancelled} orders failed to cancel! "
                          f"Placing new orders anyway, but may accumulate orders.")
        
        if should_place_new_orders and (bid_quotes or ask_quotes):
            orders_to_place = []
            
            # Tạo orders cho BID
            for (lvl, price, q) in bid_quotes:
                orders_to_place.append({
                    "symbol": symbol,
                    "side": "buy",
                    "price": price,
                    "size": int(q),
                })
            
            # Tạo orders cho ASK
            for (lvl, price, q) in ask_quotes:
                orders_to_place.append({
                    "symbol": symbol,
                    "side": "sell",
                    "price": price,
                    "size": int(q),
                })
            
            if orders_to_place:
                result_queue = queue.Queue()
                place_cmd = {
                    "action": "place",
                    "orders": orders_to_place,
                    "result_queue": result_queue,
                }
                try:
                    trading_q.put_nowait(place_cmd)
                    print(f">>> Sent {len(orders_to_place)} orders to trading queue, waiting for results...")
                    # Đợi kết quả (timeout dài hơn cho nhiều orders)
                    try:
                        place_result = result_queue.get(timeout=10.0)
                        if place_result.get("action") == "place_result":
                            place_results = place_result.get("results", [])
                            for r in place_results:
                                if r.get("success") and r.get("orderId"):
                                    new_order_ids.append(r["orderId"])
                            if new_order_ids:
                                print(f">>> Placed {len(new_order_ids)}/{len(orders_to_place)} orders successfully")
                            else:
                                print(f">>> No orders placed successfully out of {len(orders_to_place)}")
                    except queue.Empty:
                        print(f">>> Timeout waiting for order placement results (>{len(orders_to_place)} orders may be slow)")
                        # Timeout không phải lỗi nghiêm trọng, tiếp tục
                        pass
                except queue.Full:
                    # Queue đầy thì bỏ bớt command cũ
                    try:
                        trading_q.get_nowait()
                        trading_q.put_nowait(place_cmd)
                    except queue.Empty:
                        pass
        
        # Lưu order_ids cho round sau
        # QUAN TRỌNG: Chỉ lưu orders mới đã place thành công
        # Nếu có orders cũ chưa cancel xong, giữ lại để retry cancel ở round sau
        # NHƯNG không thêm orders mới vào nếu vẫn còn orders cũ chưa cancel
        if last_round_order_ids and round_id > 1:
            # Nếu cancel không thành công hoàn toàn, giữ lại orders chưa cancel để retry
            if cancel_success_count < len(last_round_order_ids):
                remaining_orders = last_round_order_ids[cancel_success_count:]
                print(f">>> WARNING: {len(remaining_orders)} orders not cancelled. Keeping for retry. "
                      f"New orders placed: {len(new_order_ids)}")
                # Giữ lại orders cũ chưa cancel + orders mới
                last_round_order_ids = remaining_orders + new_order_ids
            else:
                # Cancel thành công hoàn toàn, chỉ lưu orders mới
                last_round_order_ids = new_order_ids
        else:
            # Round đầu tiên hoặc không có orders cũ, chỉ lưu orders mới
            last_round_order_ids = new_order_ids

        # format timestamp với mili giây (bỏ bớt 3 chữ số micro cuối)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # format grid đẹp (qty giờ là integer)
        bid_str = " ".join(
            [f"L{lvl}:{price:.6f}x{int(qty)}" for (lvl, price, qty) in bid_quotes]
        ) if bid_quotes else "-"
        ask_str = " ".join(
            [f"L{lvl}:{price:.6f}x{int(qty)}" for (lvl, price, qty) in ask_quotes]
        ) if ask_quotes else "-"

        # Log 1 round
        print(
            f"[{ts_str}] {symbol} ROUND={round_id} "
            f"mid={mid_:.6f} vol_factor={vf_:.3f} "
            f"qty={qty_:.6f} equity={eq_:.6f} fills={fills_} quotes={num_quotes}"
        )
        print(f"    BIDS: {bid_str}")
        print(f"    ASKS: {ask_str}")

        # ====== GHI CSV CHO ROUND NÀY (NON-BLOCKING) ======
        csv_path = os.path.join(CSV_DIR, f"quotes_{symbol}.csv")

        # Chuẩn bị data để gửi vào CSV queue
        rows = []
        # Map order_id theo (side, price, size) từ place_results
        order_id_map = {}  # {(side, price, size): order_id}
        
        # Match results với quotes dựa trên side, price, size
        if place_results:
            # Tạo list quotes với metadata
            all_quotes = []
            for (lvl, price, q) in bid_quotes:
                all_quotes.append(("BID", lvl, price, int(q)))
            for (lvl, price, q) in ask_quotes:
                all_quotes.append(("ASK", lvl, price, int(q)))
            
            # Match results với quotes (theo thứ tự, nhưng verify bằng side/price/size)
            for idx, result in enumerate(place_results):
                if result.get("success") and result.get("orderId"):
                    order_id = result["orderId"]
                    side = result.get("side", "").lower()
                    price = result.get("price")
                    size = result.get("quantity")
                    
                    # Tìm quote tương ứng (ưu tiên match chính xác, nếu không thì theo thứ tự)
                    if idx < len(all_quotes):
                        quote_side, quote_lvl, quote_price, quote_size = all_quotes[idx]
                        # Verify match
                        if (side == "buy" and quote_side == "BID" and 
                            abs(price - quote_price) < 1e-8 and size == quote_size):
                            order_id_map[(quote_side, quote_lvl, quote_price, quote_size)] = order_id
                        elif (side == "sell" and quote_side == "ASK" and 
                              abs(price - quote_price) < 1e-8 and size == quote_size):
                            order_id_map[(quote_side, quote_lvl, quote_price, quote_size)] = order_id
                        else:
                            # Fallback: vẫn map theo thứ tự nếu không match chính xác
                            order_id_map[(quote_side, quote_lvl, quote_price, quote_size)] = order_id
        
        # mỗi mức BID một dòng
        for (lvl, price, q) in bid_quotes:
            order_id = order_id_map.get(("BID", lvl, price, int(q)), "")
            rows.append([
                ts_str,
                symbol,
                round_id,
                "BID",
                lvl,
                f"{price:.8f}",
                f"{int(q)}",          # integer size
                order_id,              # order_id
                f"{mid_:.8f}",
                f"{vf_:.6f}",
                f"{qty_:.8f}",
                f"{eq_:.8f}",
                fills_,
            ])
        # mỗi mức ASK một dòng
        for (lvl, price, q) in ask_quotes:
            order_id = order_id_map.get(("ASK", lvl, price, int(q)), "")
            rows.append([
                ts_str,
                symbol,
                round_id,
                "ASK",
                lvl,
                f"{price:.8f}",
                f"{int(q)}",          # integer size
                order_id,              # order_id
                f"{mid_:.8f}",
                f"{vf_:.6f}",
                f"{qty_:.8f}",
                f"{eq_:.8f}",
                fills_,
            ])

        # Gửi vào CSV queue (non-blocking)
        if rows:
            csv_data = {
                "symbol": symbol,
                "csv_path": csv_path,
                "header": [
                    "timestamp",
                    "symbol",
                    "round",
                    "side",
                    "level",
                    "price",
                    "qty",
                    "order_id",
                    "mid",
                    "vol_factor",
                    "inventory",
                    "equity",
                    "fills",
                ] if round_id == 1 else None,  # Chỉ ghi header ở round đầu
                "rows": rows,
            }
            try:
                csv_q.put_nowait(csv_data)
            except queue.Full:
                # Queue đầy thì bỏ bớt data cũ
                try:
                    csv_q.get_nowait()
                    csv_q.put_nowait(csv_data)
                except queue.Empty:
                    pass


# ===============================================
# 6) MAIN: GHÉP TẤT CẢ LẠI
# ===============================================

def main():
    stop_event = threading.Event()

    raw_q = queue.Queue(maxsize=RAW_QUEUE_MAXSIZE)
    tick_q = queue.Queue(maxsize=TICK_QUEUE_MAXSIZE)
    trading_q = queue.Queue(maxsize=100)  # Queue cho trading commands
    csv_q = queue.Queue(maxsize=CSV_QUEUE_MAXSIZE)  # Queue cho CSV writing

    threads = []

    # Thread 1: WebSocket collector (market data)
    ws_thread = threading.Thread(
        target=ws_thread_fn, args=(SYMBOLS, raw_q, stop_event), daemon=True, name="WS-Collector"
    )
    threads.append(ws_thread)

    # Threads 2-N: Multiple Parser workers (parallel processing dựa trên CPU cores)
    parser_threads = []
    for i in range(PARSER_WORKERS):
        parser_thread = threading.Thread(
            target=parse_loop, 
            args=(raw_q, tick_q, stop_event), 
            daemon=True, 
            name=f"Parser-{i+1}"
        )
        parser_threads.append(parser_thread)
        threads.append(parser_thread)

    # Thread: Trading manager (WebSocket trading)
    trading_thread = threading.Thread(
        target=trading_manager_thread_fn, args=(trading_q, stop_event), daemon=True, name="Trading"
    )
    threads.append(trading_thread)

    # Thread: CSV writer (non-blocking file I/O)
    csv_thread = threading.Thread(
        target=csv_writer_loop, args=(csv_q, stop_event), daemon=True, name="CSV-Writer"
    )
    threads.append(csv_thread)

    # Thread: Alpha engine (main processing)
    alpha_thread = threading.Thread(
        target=alpha_loop, args=(tick_q, trading_q, csv_q, stop_event), daemon=True, name="Alpha"
    )
    threads.append(alpha_thread)

    # Start all threads
    for t in threads:
        t.start()
    
    total_threads = len(threads)
    print(f">>> Started {total_threads} threads (1 WS, {PARSER_WORKERS} Parsers, 1 Trading, 1 CSV, 1 Alpha)")
    print(f">>> Utilizing {CPU_COUNT} CPU cores")

    print("Live tick-based MM engine started. Nhấn Ctrl+C để dừng.")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Stopping...")
        stop_event.set()

    # gửi None để các loop thoát nếu đang chờ queue
    try:
        raw_q.put_nowait(None)
    except queue.Full:
        pass
    try:
        tick_q.put_nowait(None)
    except queue.Full:
        pass
    try:
        csv_q.put_nowait(None)
    except queue.Full:
        pass

    # Join all threads (threads list đã được tạo ở trên)
    for t in threads:
        t.join(timeout=5.0)

    print("Stopped cleanly.")


if __name__ == "__main__":
    main()
