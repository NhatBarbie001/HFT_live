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
# 1. LẤY API KEY TỪ BIẾN MÔI TRƯỜNG
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
# 2. HÀM BUILD URL WEBSOCKET TRADING
# =====================================================
# Theo KuCoin Unified WS:
#   wss://wsapi.kucoin.com/v1/private?apikey=...&sign=...&passphrase=...&timestamp=...
# trong đó:
#   sign = Base64( HMAC_SHA256( secret, apikey + timestamp ) )
#   passphrase = Base64( HMAC_SHA256( secret, raw_passphrase ) )
# 

def build_ws_trading_url() -> str:
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


# =====================================================
# 3. AUTHENTICATE SESSION (KÝ CHALLENGE)
# =====================================================
# Flow từ doc WebSocket KuCoin:
# - Server gửi JSON challenge, ví dụ: {"timestamp":..., "sessionId":"..."}
# - Client HMAC-SHA256(raw_json, secret) rồi Base64 → gửi lại
# - Server trả {"data":"welcome", ...}
# 

async def authenticate_session(ws) -> None:
    challenge_raw = await ws.recv()
    print(">>> challenge:", challenge_raw)

    # Nếu server trả lỗi (có field code/msg), xử lý luôn:
    try:
        challenge = json.loads(challenge_raw)
        if "code" in challenge and challenge.get("code") != "200000":
            raise RuntimeError(f"Server trả lỗi ngay khi connect: {challenge}")
    except json.JSONDecodeError:
        # Nếu không parse được thì cứ ký trên raw (đúng spec)
        pass

    sig_bytes = hmac.new(
        API_SECRET.encode("utf-8"),
        challenge_raw.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    sig_b64 = base64.b64encode(sig_bytes).decode("utf-8")

    await ws.send(sig_b64)
    print(">>> sent auth signature")

    welcome_raw = await ws.recv()
    print(">>> welcome:", welcome_raw)

    welcome = json.loads(welcome_raw)
    if welcome.get("data") != "welcome":
        raise RuntimeError(f"Session verification FAILED: {welcome}")

    print(">>> Auth OK, sessionId =", welcome.get("sessionId"))
    print(">>> pingInterval =", welcome.get("pingInterval"), "ms")


# =====================================================
# 4. HÀM CANCEL LỆNH FUTURES BẰNG orderId
# =====================================================
# Theo doc "Cancel Order" qua WebSocket:
#   op = "futures.cancel"
#   args có thể chứa symbol + orderId hoặc symbol + clientOid
#
# Response futures by orderId:
#   {
#     "id": "request-001",
#     "op": "futures.cancel",
#     "code": "200000",
#     "data": {
#       "cancelledOrderIds": ["235303670076489728"]
#     }
#   }
# 

async def cancel_futures_order_by_order_id(ws, symbol: str, order_id: str) -> dict:
    # id request không quá 32 bytes
    req_id = f"cancel_{order_id}"[-32:]

    msg = {
        "id": req_id,
        "op": "futures.cancel",
        "args": {
            "symbol": symbol,
            "orderId": order_id,
        },
    }

    print(">>> sending cancel:", json.dumps(msg, indent=2))
    await ws.send(json.dumps(msg))

    raw = await ws.recv()
    print(">>> recv cancel:", raw)

    data = json.loads(raw)

    result = {
        "success": False,
        "symbol": symbol,
        "orderId": order_id,
        "cancelledOrderIds": [],
        "code": data.get("code"),
        "msg": data.get("msg"),
    }

    # Check đúng op/id
    if data.get("op") != "futures.cancel" or data.get("id") != req_id:
        return result

    if data.get("code") != "200000":
        # KuCoin báo lỗi (ví dụ: lệnh đã filled, đã huỷ, orderId sai, ...)
        return result

    cancelled_ids = data.get("data", {}).get("cancelledOrderIds") or []
    result["success"] = True
    result["cancelledOrderIds"] = cancelled_ids
    return result


# =====================================================
# 5. MAIN DEMO
# =====================================================

async def main():
    import sys

    if len(sys.argv) < 3:
        print("Usage: python cancel_order_ws.py <SYMBOL> <ORDER_ID>")
        print("VD:    python cancel_order_ws.py ENAUSDTM 381631676037533696")
        return

    symbol = sys.argv[1]
    order_id = sys.argv[2]

    ws_url = build_ws_trading_url()
    print("Connecting to:", ws_url)

    async with websockets.connect(ws_url) as ws:
        await authenticate_session(ws)

        res = await cancel_futures_order_by_order_id(ws, symbol, order_id)

        if res["success"]:
            print(
                f"Cancel SUCCESS: symbol={res['symbol']}, "
                f"orderId={res['orderId']}, "
                f"cancelledOrderIds={res['cancelledOrderIds']}"
            )
        else:
            print(
                f"Cancel FAIL: symbol={res['symbol']}, orderId={res['orderId']}, "
                f"code={res['code']}, msg={res['msg']}"
            )


if __name__ == "__main__":
    asyncio.run(main())
