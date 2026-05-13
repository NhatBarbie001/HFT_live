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
import numpy as np
from numba import njit
from scipy.stats import randint as sp_randint
from sklearn.model_selection import RandomizedSearchCV
import pandas as pd

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
# 3) PARSER TỪ RAW WS → TICK ĐƠN GIẢN (TỐI ƯU VỚI THREADPOOL)
# ===============================================

def parse_single_message(msg_item):
    """Parse một message thành tick - dùng cho ThreadPoolExecutor."""
    topic = msg_item.get("topic", "")
    if not topic:
        return None
    
    try:
        symbol = topic.split(":")[-1]
    except Exception:
        return None
    
    data = msg_item.get("data", {})
    ts_ms = data.get("timestamp")
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    
    if ts_ms is None or not bids or not asks:
        return None
    
    try:
        ts = datetime.utcfromtimestamp(ts_ms / 1000.0)
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
    except Exception:
        return None
    
    return {
        "timestamp": ts,
        "symbol": symbol,
        "best_bid": best_bid,
        "best_ask": best_ask,
    }


def parse_loop(raw_q: queue.Queue, tick_q: queue.Queue, stop_event: threading.Event, executor: ThreadPoolExecutor):
    """
    Đọc raw message từ RAW_QUEUE, parse thành:
        {timestamp, symbol, best_bid, best_ask}
    rồi đẩy vào TICK_QUEUE.
    Tối ưu: batch processing với ThreadPoolExecutor để parse song song.
    """
    batch = []
    batch_size = 100
    last_batch_time = time.time()
    pending_futures = []  # Lưu futures đang chạy
    
    while not stop_event.is_set():
        try:
            # Lấy message với timeout ngắn
            msg = raw_q.get(timeout=0.05)
            if msg is None:
                # Flush batch trước khi break
                if batch:
                    futures = [executor.submit(parse_single_message, msg_item) for msg_item in batch]
                    for future in futures:
                        try:
                            tick = future.result(timeout=0.1)
                            if tick:
                                try:
                                    tick_q.put_nowait(tick)
                                except queue.Full:
                                    try:
                                        tick_q.get_nowait()
                                        tick_q.put_nowait(tick)
                                    except queue.Empty:
                                        pass
                        except Exception:
                            pass
                break
            
            batch.append(msg)
            
            # Xử lý batch khi đủ size hoặc timeout
            now = time.time()
            if len(batch) >= batch_size or (now - last_batch_time) > 0.01:
                # Submit batch để parse song song
                futures = [executor.submit(parse_single_message, msg_item) for msg_item in batch]
                pending_futures.extend(futures)
                batch = []
                last_batch_time = now
            
            # Xử lý các futures đã hoàn thành (non-blocking)
            completed = []
            for future in pending_futures:
                if future.done():
                    completed.append(future)
                    try:
                        tick = future.result(timeout=0.001)
                        if tick:
                            try:
                                tick_q.put_nowait(tick)
                            except queue.Full:
                                try:
                                    tick_q.get_nowait()
                                    tick_q.put_nowait(tick)
                                except queue.Empty:
                                    pass
                    except Exception:
                        pass
            
            # Xóa futures đã hoàn thành
            for f in completed:
                pending_futures.remove(f)
                
        except queue.Empty:
            # Xử lý pending futures
            completed = []
            for future in pending_futures:
                if future.done():
                    completed.append(future)
                    try:
                        tick = future.result(timeout=0.001)
                        if tick:
                            try:
                                tick_q.put_nowait(tick)
                            except queue.Full:
                                try:
                                    tick_q.get_nowait()
                                    tick_q.put_nowait(tick)
                                except queue.Empty:
                                    pass
                    except Exception:
                        pass
            
            for f in completed:
                pending_futures.remove(f)
            
            # Flush batch nếu có và đã timeout
            if batch and (time.time() - last_batch_time) > 0.05:
                futures = [executor.submit(parse_single_message, msg_item) for msg_item in batch]
                for future in futures:
                    try:
                        tick = future.result(timeout=0.1)
                        if tick:
                            try:
                                tick_q.put_nowait(tick)
                            except queue.Full:
                                try:
                                    tick_q.get_nowait()
                                    tick_q.put_nowait(tick)
                                except queue.Empty:
                                    pass
                    except Exception:
                        pass
                batch = []
                last_batch_time = time.time()
            continue


# ===============================================
# 4) LIVE GRID ENGINE STATEFUL (DÙNG LOGIC TỪ BACKTEST)
# ===============================================

@njit
def snap_down(price, tick_size):
    if price <= 0.0:
        return 0.0
    ticks = np.floor(price / tick_size + 1e-12)
    return ticks * tick_size


@njit
def snap_up(price, tick_size):
    if price <= 0.0:
        return 0.0
    ticks = np.ceil(price / tick_size - 1e-12)
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
        half_spread: float,  # trực tiếp theo giá
        price_range: float,  # trực tiếp theo giá
        grid_num: int,
        update_threshold_ticks: int,
        dollar_qty: float = 10.0,
    ):
        self.fee = float(fee)
        self.max_position = float(max_position)
        self.half_spread = float(half_spread)
        self.price_range = float(price_range)
        self.grid_num = int(grid_num)
        self.update_threshold_price = update_threshold_ticks * TICK_SIZE

        self.dollar_qty = float(dollar_qty)

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

    def _rebuild_grid(self, mid: float, best_bid: float, best_ask: float, vol_factor: float):
        """
        Dựng lưới giá theo logic từ backtest:
        - Không dùng vol_factor scaling
        - Không dùng inventory skew exp
        - Chỉ dùng soft cap và hard cap
        """
        # Giá trị inventory theo USD
        pos_value = self.running_qty * mid
        denom = self.max_position if self.max_position != 0.0 else 1.0
        x = pos_value / denom  # tỷ lệ so với max_position

        # =========================
        # XÁC ĐỊNH SỐ LEVEL MỖI PHÍA
        # =========================
        nb = self.grid_num
        na = self.grid_num

        # Hard-cap: nếu quá trần → tắt hẳn một phía
        if x >= 1.0:
            nb = 0  # không mua thêm
        elif x <= -1.0:
            na = 0  # không bán thêm
        else:
            # Soft-cap: khi x vượt soft_cap thì giảm dần số levels bên đó
            if x > self.soft_cap:
                # giảm tuyến tính từ soft_cap -> 1.0
                ratio = (1.0 - x) / (1.0 - self.soft_cap)  # từ 1 → 0
                if ratio < 0.0:
                    ratio = 0.0
                nb = int(self.grid_num * ratio)
            elif x < -self.soft_cap:
                ratio = (1.0 + x) / (1.0 - self.soft_cap)  # đối xứng
                if ratio < 0.0:
                    ratio = 0.0
                na = int(self.grid_num * ratio)

        if nb < 0:
            nb = 0
        if na < 0:
            na = 0

        self.current_nb = nb
        self.current_na = na

        # =========================
        # DỰNG LƯỚI GIÁ
        # =========================
        if self.grid_num > 1:
            step = self.price_range / (self.grid_num - 1)
            if step < 0.0:
                step = 0.0
        else:
            step = 0.0

        # BID levels: đặt dưới mid, không vượt quá best_bid (để giữ passive)
        for j in range(nb):
            offset = self.half_spread + j * step
            raw_price = mid - offset
            if raw_price > best_bid:
                raw_price = best_bid
            bid_price = snap_down(raw_price, TICK_SIZE)
            if bid_price <= 0.0:
                bid_price = 0.0
            self.bid_prices[j] = bid_price

        # ASK levels: đặt trên mid, không thấp hơn best_ask
        for j in range(na):
            offset = self.half_spread + j * step
            raw_price = mid + offset
            if raw_price < best_ask:
                raw_price = best_ask
            ask_price = snap_up(raw_price, TICK_SIZE)
            if ask_price <= 0.0:
                ask_price = 0.0
            self.ask_prices[j] = ask_price

        # Clear unused levels
        for j in range(nb, MAX_GRID_LEVELS):
            self.bid_prices[j] = 0.0
        for j in range(na, MAX_GRID_LEVELS):
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

        # BID fills: nếu low <= bid_price
        if self.current_nb > 0:
            for j in range(self.current_nb):
                bp = self.bid_prices[j]
                if bp <= 0.0:
                    continue
                if low <= bp:
                    order_qty = 1.0  # Cố định 1 ENA
                    self.running_qty += order_qty
                    self.static_equity -= bp * order_qty
                    self.fee_paid += bp * order_qty * self.fee
                    self.fills += 1

        # ASK fills: nếu high >= ask_price
        if self.current_na > 0:
            for j in range(self.current_na):
                ap = self.ask_prices[j]
                if ap <= 0.0:
                    continue
                if high >= ap:
                    order_qty = 1.0  # Cố định 1 ENA
                    self.running_qty -= order_qty
                    self.static_equity += ap * order_qty
                    self.fee_paid += ap * order_qty * self.fee
                    self.fills += 1

        equity = self.static_equity + self.running_qty * mid - self.fee_paid

        # Chuẩn bị list quote + quantity cho từng level
        # Quantity cố định 1 ENA cho mỗi lệnh
        bid_quotes = []
        for j in range(self.current_nb):
            p = self.bid_prices[j]
            if p > 0.0:
                q = 1  # Cố định 1 ENA
                bid_quotes.append((j, p, q))

        ask_quotes = []
        for j in range(self.current_na):
            p = self.ask_prices[j]
            if p > 0.0:
                q = 1  # Cố định 1 ENA
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
        half_spread=0.000200,  # trực tiếp theo giá, không dùng ticks
        price_range=0.005200,  # trực tiếp theo giá, không dùng ticks
        grid_num=14,
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
    pending_cancel_result_queue = None  # Queue kết quả cancel từ round trước
    pending_place_result_queue = None  # Queue kết quả place từ round trước

    print("Alpha loop started (tick-by-tick, quote-on-GRID-change, non-blocking trading).")

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

        # Check kết quả place từ round trước (nếu có timeout)
        if pending_place_result_queue:
            try:
                place_result = pending_place_result_queue.get_nowait()
                if place_result.get("action") == "place_result":
                    results = place_result.get("results", [])
                    placed = [r for r in results if r.get("success") and r.get("orderId")]
                    if placed:
                        # Cập nhật last_round_order_ids với orders đã place thành công
                        new_ids = [r["orderId"] for r in placed]
                        # Orders cũ đã được cancel ở round trước, chỉ lưu orders mới
                        last_round_order_ids = new_ids
                        print(f">>> [Async] Placed {len(placed)} orders from previous round (delayed result)")
            except queue.Empty:
                # Vẫn chưa có kết quả, giữ lại để check ở round sau
                pass
            else:
                # Đã có kết quả hoặc timeout, clear pending queue
                pending_place_result_queue = None

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
        # Đợi với timeout ngắn (5s) để tránh block lâu nhưng vẫn đảm bảo cancel xong
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
                print(f">>> Cancelling {len(last_round_order_ids)} orders from previous round...")
                # Đợi kết quả với timeout ngắn (5s) để đảm bảo cancel hoàn tất trước khi place mới
                try:
                    cancel_result = result_queue.get(timeout=5.0)
                    if cancel_result.get("action") == "cancel_result":
                        results = cancel_result.get("results", [])
                        cancelled = [r for r in results if r.get("success")]
                        failed = [r for r in results if not r.get("success")]
                        cancel_success_count = len(cancelled)
                        if cancelled:
                            print(f">>> Cancelled {len(cancelled)}/{len(last_round_order_ids)} orders successfully")
                        if failed:
                            print(f">>> WARNING: {len(failed)} orders failed to cancel: {[r.get('orderId') for r in failed[:3]]}")
                            if len(failed) > len(cancelled):
                                print(f">>> ERROR: More orders failed to cancel than succeeded! May have orphaned orders.")
                        # Cập nhật last_round_order_ids: xóa orders đã cancel thành công
                        if cancelled:
                            cancelled_ids = {r.get("orderId") for r in cancelled}
                            last_round_order_ids = [oid for oid in last_round_order_ids if oid not in cancelled_ids]
                except queue.Empty:
                    print(f">>> WARNING: Timeout waiting for cancel results (5s). Orders may not be cancelled.")
                    # Timeout - không chắc orders đã được cancel, nhưng vẫn tiếp tục
                    # Giữ nguyên last_round_order_ids để retry ở round sau
                    pass
            except queue.Full:
                # Queue đầy thì bỏ bớt command cũ
                try:
                    trading_q.get_nowait()
                    trading_q.put_nowait(cancel_cmd)
                    # Vẫn đợi kết quả
                    try:
                        cancel_result = result_queue.get(timeout=5.0)
                        if cancel_result.get("action") == "cancel_result":
                            results = cancel_result.get("results", [])
                            cancelled = [r for r in results if r.get("success")]
                            failed = [r for r in results if not r.get("success")]
                            cancel_success_count = len(cancelled)
                            if cancelled:
                                print(f">>> Cancelled {len(cancelled)}/{len(last_round_order_ids)} orders successfully")
                            if failed:
                                print(f">>> WARNING: {len(failed)} orders failed to cancel")
                            if cancelled:
                                cancelled_ids = {r.get("orderId") for r in cancelled}
                                last_round_order_ids = [oid for oid in last_round_order_ids if oid not in cancelled_ids]
                    except queue.Empty:
                        print(f">>> WARNING: Timeout waiting for cancel results (5s).")
                        pass
                except queue.Empty:
                    pass
        
        # ====== PLACE ORDERS MỚI ======
        # CHỈ PLACE SAU KHI ĐÃ CANCEL XONG (hoặc không có orders cũ)
        new_order_ids = []
        place_results = []  # Lưu toàn bộ results để map chính xác
        
        # Nếu có orders cũ cần cancel nhưng cancel fail nhiều, cảnh báo
        should_place_new_orders = True
        if last_round_order_ids and round_id > 1:
            expected_cancelled = len(last_round_order_ids)
            if cancel_success_count < expected_cancelled * 0.5:  # Nếu cancel < 50% thành công
                print(f">>> WARNING: Only {cancel_success_count}/{expected_cancelled} orders cancelled. "
                      f"May have {expected_cancelled - cancel_success_count} orphaned orders.")
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
                    print(f">>> Placing {len(orders_to_place)} orders...")
                    # Đợi kết quả với timeout ngắn (5s) để lấy order_ids ngay
                    try:
                        place_result = result_queue.get(timeout=5.0)
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
                        print(f">>> WARNING: Timeout waiting for order placement results (5s). "
                              f"Will check in next round.")
                        # Timeout - sẽ check ở round sau nếu cần
                        pending_place_result_queue = result_queue
                except queue.Full:
                    # Queue đầy thì bỏ bớt command cũ
                    try:
                        trading_q.get_nowait()
                        trading_q.put_nowait(place_cmd)
                        try:
                            place_result = result_queue.get(timeout=5.0)
                            if place_result.get("action") == "place_result":
                                place_results = place_result.get("results", [])
                                for r in place_results:
                                    if r.get("success") and r.get("orderId"):
                                        new_order_ids.append(r["orderId"])
                                if new_order_ids:
                                    print(f">>> Placed {len(new_order_ids)}/{len(orders_to_place)} orders successfully")
                        except queue.Empty:
                            pending_place_result_queue = result_queue
                    except queue.Empty:
                        pass
        
        # Lưu order_ids cho round sau
        # Nếu có new_order_ids từ kết quả ngay lập tức, cập nhật last_round_order_ids
        # Nếu có pending_place_result_queue, sẽ cập nhật ở round sau khi có kết quả
        if new_order_ids:
            # Có orders mới từ kết quả ngay lập tức
            if last_round_order_ids and round_id > 1:
                # Đã có orders cũ, chỉ lưu orders mới (orders cũ đã được cancel ở trên)
                last_round_order_ids = new_order_ids
            else:
                # Không có orders cũ, chỉ lưu orders mới
                last_round_order_ids = new_order_ids
        elif pending_place_result_queue:
            # Chưa có kết quả, sẽ cập nhật ở round sau
            # Giữ nguyên last_round_order_ids để cancel ở round sau nếu cần
            pass
        # Nếu không có new_order_ids và không có pending_place_result_queue, 
        # last_round_order_ids đã được cập nhật ở phần cancel ở trên

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
        csv_path = os.path.join(CSV_DIR, f"quotes_{symbol}_2.csv")

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
    
    # ThreadPoolExecutor cho parser (tối ưu tốc độ parse)
    parser_executor = ThreadPoolExecutor(max_workers=PARSER_WORKERS, thread_name_prefix="ParserPool")

    # Thread 1: WebSocket collector (market data)
    ws_thread = threading.Thread(
        target=ws_thread_fn, args=(SYMBOLS, raw_q, stop_event), daemon=True, name="WS-Collector"
    )
    threads.append(ws_thread)

    # Threads 2-N: Multiple Parser workers (parallel processing với ThreadPoolExecutor)
    parser_threads = []
    for i in range(PARSER_WORKERS):
        parser_thread = threading.Thread(
            target=parse_loop, 
            args=(raw_q, tick_q, stop_event, parser_executor), 
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
    
    # Shutdown ThreadPoolExecutor
    parser_executor.shutdown(wait=True, timeout=5.0)

    print("Stopped cleanly.")


if __name__ == "__main__":
    main()
