# HFT Live Trading System - KuCoin Futures Market Making

Hệ thống High-Frequency Trading (HFT) tự động cho KuCoin Futures, thực hiện chiến lược Market Making dựa trên Grid với quản lý inventory động.

## 📋 Mục lục

- [Tổng quan](#tổng-quan)
- [Tính năng chính](#tính-năng-chính)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Cấu hình](#cấu-hình)
- [Sử dụng](#sử-dụng)
- [Giải thích các thành phần](#giải-thích-các-thành-phần)
- [Lưu ý quan trọng](#lưu-ý-quan-trọng)

## 🎯 Tổng quan

Hệ thống này thực hiện chiến lược Market Making tự động trên KuCoin Futures với các đặc điểm:

- **Real-time orderbook processing**: Nhận và xử lý orderbook data từ KuCoin Futures qua WebSocket
- **Grid-based market making**: Tự động đặt lệnh mua/bán theo lưới giá với quản lý inventory
- **Volatility-adaptive**: Điều chỉnh spread và grid dựa trên volatility thị trường
- **Multi-threaded architecture**: Xử lý song song với nhiều threads để tối ưu hiệu suất
- **Order management**: Tự động hủy và đặt lại lệnh khi grid thay đổi

## ✨ Tính năng chính

### 1. Market Data Collection
- Kết nối WebSocket với KuCoin Futures để nhận orderbook level 2 (50 levels)
- Parse và xử lý tick data real-time
- Batch processing để tối ưu hiệu suất

### 2. Grid Engine
- **LiveGridEngine**: Engine tính toán grid giá tự động
- Inventory management với hard cap và soft cap
- Volatility factor điều chỉnh spread động
- Tick size rounding để đảm bảo giá hợp lệ

### 3. Trading Execution
- Đặt/hủy lệnh qua WebSocket trading API
- Quản lý order lifecycle tự động
- Retry logic khi connection bị gián đoạn

### 4. Data Logging
- Ghi log quotes vào CSV theo từng round
- Lưu trữ order IDs, prices, quantities, inventory, equity
- Non-blocking CSV writer để không ảnh hưởng performance

## 📁 Cấu trúc dự án

```
hft_lives/
├── alpha.py                          # Phiên bản chính của trading bot
├── alpha_2.py                        # Phiên bản tối ưu với ThreadPoolExecutor
├── get_data.py                      # Script thu thập orderbook data
├── get_data_future_orderbook_kucoin.py  # Script thu thập futures orderbook
├── test_order.py                    # Script test đặt lệnh
├── test_cancel.py                   # Script test hủy lệnh
├── data/                            # Thư mục lưu orderbook data
│   └── ENAUSDTM_kucoin_orderbook.csv
├── quotes_logs/                     # Thư mục lưu log quotes
│   └── quotes_ENAUSDTM.csv
├── alpha.log                        # Log file cho alpha.py
├── alpha_2.log                      # Log file cho alpha_2.py
└── README.md                        # File này
```

## 💻 Yêu cầu hệ thống

- **Python**: 3.7+
- **OS**: Linux, macOS, hoặc Windows
- **CPU**: Khuyến nghị 4+ cores để tận dụng multi-threading
- **RAM**: Tối thiểu 2GB
- **Network**: Kết nối internet ổn định với latency thấp đến KuCoin

## 📦 Cài đặt

### 1. Clone repository

```bash
git clone <repository-url>
cd hft_lives
```

### 2. Cài đặt dependencies

```bash
pip install websockets requests numpy numba scipy scikit-learn pandas
```

Hoặc sử dụng `requirements.txt` (nếu có):

```bash
pip install -r requirements.txt
```

### 3. Cấu hình API credentials

**⚠️ QUAN TRỌNG**: Hệ thống sử dụng biến môi trường để bảo mật API credentials. Bạn cần set các biến môi trường sau:

#### Cách 1: Export trực tiếp (Linux/macOS)

```bash
export KUCOIN_API_KEY='your_api_key'
export KUCOIN_API_SECRET='your_api_secret'
export KUCOIN_API_PASSPHRASE='your_passphrase'
export KUCOIN_MARGIN_MODE='ISOLATED'  # hoặc 'CROSSED'
```

#### Cách 2: Sử dụng file .env (Khuyến nghị)

1. Tạo file `.env` trong thư mục gốc:
```bash
cat > .env << EOF
KUCOIN_API_KEY=your_api_key
KUCOIN_API_SECRET=your_api_secret
KUCOIN_API_PASSPHRASE=your_passphrase
KUCOIN_MARGIN_MODE=ISOLATED
EOF
```

2. Load biến môi trường từ file `.env`:
```bash
# Linux/macOS
export $(cat .env | xargs)

# Hoặc sử dụng python-dotenv (cần cài đặt: pip install python-dotenv)
# Thêm vào đầu các file Python:
# from dotenv import load_dotenv
# load_dotenv()
```

**⚠️ CẢNH BÁO**: 
- File `.env` đã được thêm vào `.gitignore` và sẽ không được commit
- KHÔNG BAO GIỜ commit API credentials vào git!
- Giới hạn quyền API key (chỉ trading, không withdraw)

## ⚙️ Cấu hình

### Cấu hình cơ bản trong `alpha.py`:

```python
# Symbols để trade
RAW_SYMBOLS = ["ENAUSDT"]  # Thêm symbols khác nếu cần

# Grid parameters
TICK_SIZE = 0.0001
MAX_GRID_LEVELS = 128

# Queue sizes
RAW_QUEUE_MAXSIZE = 50000
TICK_QUEUE_MAXSIZE = 50000
CSV_QUEUE_MAXSIZE = 1000

# CPU configuration
CPU_COUNT = os.cpu_count() or 4
PARSER_WORKERS = max(1, CPU_COUNT - 2)
```

### Cấu hình Grid Engine:

Trong hàm `alpha_loop()`, bạn có thể điều chỉnh:

```python
engine = LiveGridEngine(
    fee=0.0002,                    # Trading fee (0.02%)
    max_position=100.0,           # Max position size (USD)
    half_spread_ticks=3,           # Half spread in ticks
    price_range_ticks=105,         # Price range for grid
    grid_num=5,                    # Số levels mỗi phía
    update_threshold_ticks=1,      # Threshold để rebuild grid
    dollar_qty=10.0,              # Dollar quantity per order
)
```

### Cấu hình Volatility:

```python
lambda_ = 0.94        # EWMA decay factor
clip_min = 0.5        # Min volatility factor
clip_max = 2.0        # Max volatility factor
```

## 🚀 Sử dụng

### 1. Chạy trading bot chính

```bash
python alpha.py
```

Hoặc phiên bản tối ưu:

```bash
python alpha_2.py
```

Bot sẽ:
- Kết nối WebSocket để nhận orderbook data
- Tính toán grid và đặt lệnh tự động
- Ghi log vào `quotes_logs/quotes_<SYMBOL>.csv`
- In thông tin ra console

### 2. Thu thập orderbook data

```bash
python get_data_future_orderbook_kucoin.py
```

Script này sẽ lưu orderbook data vào `data/` directory.

### 3. Test đặt lệnh

```bash
python test_order.py
```

### 4. Test hủy lệnh

```bash
python test_cancel.py <SYMBOL> <ORDER_ID>
# Ví dụ:
python test_cancel.py ENAUSDTM 381631676037533696
```

### Dừng bot

Nhấn `Ctrl+C` để dừng bot một cách an toàn. Bot sẽ:
- Hủy tất cả orders đang mở
- Đóng các connections
- Ghi log cuối cùng

## 🔧 Giải thích các thành phần

### 1. WebSocket Client (`kucoin_ws_client`)

- Kết nối với KuCoin Futures WebSocket
- Subscribe vào orderbook level 2
- Xử lý ping/pong để maintain connection
- Auto-reconnect khi mất kết nối

### 2. Parser (`parse_loop`)

- Parse raw WebSocket messages thành tick data
- Batch processing để tối ưu hiệu suất
- Multi-threaded parsing (trong `alpha_2.py`)

### 3. LiveGridEngine

**Chức năng chính:**
- Tính toán grid giá dựa trên mid price
- Quản lý inventory với hard cap và soft cap
- Inventory skew để điều chỉnh spread
- Simulate fills dựa trên best bid/ask

**Grid logic:**
- Hard cap: Khi inventory đạt max → tắt một phía
- Soft cap: Giảm dần số levels khi inventory gần max
- Inventory skew: Điều chỉnh spread theo inventory

### 4. Trading Manager (`trading_manager_loop`)

- Quản lý WebSocket trading connection
- Xử lý đặt/hủy lệnh tuần tự (WebSocket không hỗ trợ concurrent)
- Retry logic khi connection bị đóng

### 5. Alpha Loop (`alpha_loop`)

**Flow chính:**
1. Nhận tick từ queue
2. Tính volatility factor (EWMA)
3. Update grid engine
4. **Chỉ khi grid thay đổi**:
   - Hủy orders cũ
   - Đặt orders mới
   - Ghi log CSV

**Tối ưu:**
- Chỉ tạo round mới khi grid thay đổi (không spam orders)
- Non-blocking CSV writing
- Timeout ngắn để responsive

## ⚠️ Lưu ý quan trọng

### 1. Rủi ro

- **Đây là hệ thống trading thực với tiền thật**. Sử dụng với rủi ro của bạn.
- Test kỹ trên testnet trước khi dùng với tiền thật.
- Monitor bot liên tục khi chạy lần đầu.

### 2. API Credentials

- **KHÔNG BAO GIỜ** commit API credentials vào git
- Sử dụng biến môi trường hoặc file config riêng
- Giới hạn quyền API key (chỉ trading, không withdraw)

### 3. Performance

- Bot được tối ưu cho low-latency trading
- Cần kết nối internet ổn định với latency thấp
- Monitor CPU và memory usage

### 4. Order Management

- Bot tự động hủy orders cũ trước khi đặt mới
- Nếu cancel fail, có thể tích lũy orders → monitor cẩn thận
- Check logs để đảm bảo orders được cancel thành công

### 5. Grid Configuration

- Điều chỉnh `grid_num`, `half_spread_ticks`, `price_range_ticks` phù hợp với symbol
- Test với số tiền nhỏ trước
- Monitor inventory để tránh over-exposure

### 6. CSV Logs

- Logs được ghi vào `quotes_logs/quotes_<SYMBOL>.csv`
- Format: timestamp, symbol, round, side, level, price, qty, order_id, mid, vol_factor, inventory, equity, fills
- Có thể dùng để backtest và phân tích

## 📊 Monitoring

### Console Output

Bot sẽ in thông tin mỗi round:
```
[2024-01-01 12:00:00.123] ENAUSDTM ROUND=1 mid=0.220000 vol_factor=1.000 qty=0.000000 equity=0.000000 fills=0 quotes=10
    BIDS: L0:0.219700x1 L1:0.219600x1 L2:0.219500x1 L3:0.219400x1 L4:0.219300x1
    ASKS: L0:0.220300x1 L1:0.220400x1 L2:0.220500x1 L3:0.220600x1 L4:0.220700x1
```

### CSV Logs

Check file `quotes_logs/quotes_<SYMBOL>.csv` để xem chi tiết:
- Tất cả orders đã đặt
- Order IDs
- Inventory và equity theo thời gian

## 🐛 Troubleshooting

### Connection Issues

- **Lỗi**: "Mất kết nối WS, reconnect..."
- **Giải pháp**: Check internet connection, firewall settings

### Order Placement Failures

- **Lỗi**: "Order failed: code=400001"
- **Giải pháp**: Check API credentials, margin mode, symbol format

### High CPU Usage

- **Giải pháp**: Giảm `PARSER_WORKERS` hoặc `batch_size`

### Queue Full

- **Lỗi**: Queue đầy, mất data
- **Giải pháp**: Tăng `RAW_QUEUE_MAXSIZE` hoặc `TICK_QUEUE_MAXSIZE`

## 📝 License

[Thêm license của bạn ở đây]

## 👤 Author

[Thêm thông tin tác giả]

## 🙏 Acknowledgments

- KuCoin API documentation
- Python websockets library
- Numba for performance optimization

---

**⚠️ DISCLAIMER**: Trading cryptocurrencies có rủi ro cao. Sử dụng hệ thống này với trách nhiệm của bạn. Tác giả không chịu trách nhiệm cho bất kỳ tổn thất tài chính nào.

