import os
import time
import hmac
import json
import base64
import hashlib
import asyncio
import urllib.parse

import websockets


# =====================================================
# LẤY API KEY TỪ BIẾN MÔI TRƯỜNG
# =====================================================

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


# =====================================================
# HÀM TẠO URL KẾT NỐI WS TRADING
# =====================================================

def build_ws_trading_url() -> str:
    """
    Tạo URL:
      wss://wsapi.kucoin.com/v1/private?apikey=xxx&sign=xxx&passphrase=xxx&timestamp=xxx

    sign = Base64(HMAC_SHA256(secret, apikey + timestamp))
    passphrase = Base64(HMAC_SHA256(secret, raw_passphrase))
    """
    ts_ms = str(int(time.time() * 1000))

    # sign for URL: HMAC(secret, apikey + timestamp)
    prehash = API_KEY + ts_ms
    sign_bytes = hmac.new(
        API_SECRET.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    sign_b64 = base64.b64encode(sign_bytes).decode("utf-8")

    # encrypted passphrase
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


# =====================================================
# HÀM AUTHENTICATE SAU KHI NHẬN SESSION CHALLENGE
# =====================================================

async def authenticate_session(ws: websockets.WebSocketClientProtocol) -> None:
    """
    Flow theo tài liệu KuCoin:
    1) Server gửi JSON challenge, ví dụ:
         {"sessionId":"...", "timestamp": 1742175983882}
    2) Client dùng API_SECRET để HMAC-SHA256 **toàn bộ chuỗi JSON raw đó**,
       Base64-encode kết quả → auth_sig
    3) Gửi auth_sig qua WebSocket
    4) Chờ server trả JSON:
         {"sessionId":"...", "data": "welcome", "pingInterval": 18000, "pingTimeout": 10000}
    """

    # 1) Nhận challenge
    challenge_raw = await ws.recv()
    print(">>> challenge from server:", challenge_raw)

    # 2) Ký trên chuỗi raw JSON challenge
    auth_sig_bytes = hmac.new(
        API_SECRET.encode("utf-8"),
        challenge_raw.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    auth_sig_b64 = base64.b64encode(auth_sig_bytes).decode("utf-8")

    # 3) Gửi chữ ký trở lại server
    #    Tài liệu nói "Send it to the server", hiểu đúng nghĩa: gửi chuỗi Base64 signature.
    #    Nếu KuCoin sau này yêu cầu format JSON khác, chỉ cần thay chỗ này.
    await ws.send(auth_sig_b64)
    print(">>> sent auth signature")

    # 4) Đọc welcome message
    welcome_raw = await ws.recv()
    print(">>> welcome msg:", welcome_raw)

    try:
        welcome = json.loads(welcome_raw)
    except json.JSONDecodeError:
        raise RuntimeError("Không parse được JSON welcome từ server, hãy print ra để xem chi tiết.")

    if welcome.get("data") != "welcome":
        raise RuntimeError(f"Session verification FAILED, server trả: {welcome}")

    print(">>> Authentication OK, sessionId =", welcome.get("sessionId"))
    print(">>> pingInterval =", welcome.get("pingInterval"), "ms")


# =====================================================
# HÀM GỬI LỆNH LIMIT BUY ENAUSDTM @ 0.22, QTY=1
# =====================================================

async def place_ena_limit_buy(ws, price="0.22", size=1):
    now_ms = int(time.time() * 1000)

    # clientOid chỉ dùng chữ/số/_/-
    client_oid = f"ena_buy_{str(price).replace('.', '')}_{now_ms}"

    symbol = "ENAUSDTM"
    side = "buy"
    order_type = "limit"

    msg = {
        "id": client_oid,
        "op": "futures.order",
        "args": {
            "clientOid": client_oid,
            "symbol": symbol,
            "marginMode": "ISOLATED",
            "leverage": 1,
            "positionSide": "BOTH",

            "side": side,
            "type": order_type,
            "size": size,
            "price": str(price),
            "timeInForce": "GTC",
            "reduceOnly": False,

            "timestamp": now_ms,
        }
    }

    print(">>> sending order:", json.dumps(msg, indent=2))
    await ws.send(json.dumps(msg))

    raw = await ws.recv()
    print(">>> recv:", raw)

    data = json.loads(raw)

    # Mặc định: fail
    result = {
        "success": False,
        "symbol": symbol,
        "side": side,
        "price": float(price),
        "quantity": size,
        "orderId": None,
        "clientOid": client_oid,
        "code": data.get("code"),
        "msg": data.get("msg"),
    }

    # Check đúng response futures.order
    if data.get("op") != "futures.order" or data.get("id") != client_oid:
        return result  # sai format thì coi như thất bại

    code = data.get("code")
    if code != "200000":
        # KuCoin trả lỗi, không có orderId
        result["msg"] = data.get("msg")
        return result

    # Thành công
    order_id = data.get("data", {}).get("orderId")
    if order_id:
        result["success"] = True
        result["orderId"] = order_id

    return result


# =====================================================
# MAIN
# =====================================================

async def main():
    ws_url = build_ws_trading_url()
    print("Connecting to:", ws_url)

    async with websockets.connect(ws_url) as ws:
        await authenticate_session(ws)

        # Đặt lệnh
        result = await place_ena_limit_buy(ws, price="0.22", size=1)

        if result["success"]:
            print(
                f"Order SUCCESS: symbol={result['symbol']}, "
                f"side={result['side']}, "
                f"price={result['price']}, "
                f"qty={result['quantity']}, "
                f"orderId={result['orderId']}"
            )
        else:
            print(
                f"Order FAIL: symbol={result['symbol']}, "
                f"side={result['side']}, "
                f"price={result['price']}, "
                f"qty={result['quantity']}, "
                f"code={result['code']}, msg={result['msg']}"
            )

if __name__ == "__main__":
    asyncio.run(main())
