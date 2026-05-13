# Architecture for Market Making — hft_live (Rust refactor)

Mục tiêu: cung cấp bản thiết kế rõ ràng để refactor `hft_live` sang Rust, tập trung vào quản lý vị thế (inventory), pending orders, vòng đời order, và an toàn race/consistency.

**Phạm vi**: domain models, order lifecycle, reconciliation loop (mỗi tick), persistence local, exchange adapter interface, risk checks, testing.

**1. Khái niệm cốt lõi**
- **Filled position**: khối lượng đã khớp (filled size).
- **Pending exposure**: khối lượng còn treo từ limit orders (remaining size).
- **Total exposure**: filled + pending. Strategy phải đọc cả hai trước khi ra quyết định.

**2. Data models (đề xuất Rust structs/enums)**
- **Order**: `struct Order { id: String, client_id: Option<String>, side: Side, price: Decimal, size: Decimal, size_filled: Decimal, remaining_size: Decimal, status: OrderStatus, created_at: DateTime<Utc>, updated_at: DateTime<Utc> }`
- **OrderStatus**: `enum OrderStatus { Open, PartiallyFilled, Filled, Cancelled }`
- **Inventory**: `struct Inventory { filled_buy: Decimal, filled_sell: Decimal, pending_buy: Decimal, pending_sell: Decimal }`
- **Exposure**: helper methods: `fn net_filled(&self) -> Decimal { filled_buy - filled_sell }`, `fn net_pending(&self) -> Decimal { pending_buy - pending_sell }`, `fn net_total(&self) -> Decimal { net_filled + net_pending }`.

Ghi chú: dùng `rust_decimal::Decimal` hoặc `i128` + scale để tránh float imprecision.

**3. Order lifecycle & state machine**
- States: LocalOpen → (ExchangeAck?) → Open / PartiallyFilled → Filled → (Final) OR CancelRequested → Cancelled
- Luôn cập nhật dựa trên exchange state (exchange is source of truth). Local state dùng để tối ưu quyết định / giảm latency.
- Khi tạo order: thêm vào local store (LocalOpen) ngay lập tức, gửi tới exchange async, đánh dấu `created_at`/`client_id`.
- Polling: fetch user orders / fills từ exchange, reconcile vào local (cập nhật `size_filled`, `remaining_size`, `status`).

**4. Quy tắc đặt lệnh (Market Making)**
- Mỗi side (BUY / SELL) chỉ có tối đa `N` active orders — enforce ở layer strategy/engine.
- Tránh duplicate orders: kiểm tra khoảng cách giá (tick / tolerance), volume overlap.
- Luôn validate: `|net_filled + net_pending| <= inventory_limit` trước khi gửi lệnh.

**5. Reconciliation loop (flow chuẩn mỗi tick)**
1. Fetch orderbook và user orders/fills từ exchange.
2. Reconcile exchange state -> local order store (update `size_filled`, `remaining_size`, `status`).
3. Tính `net_filled`, `net_pending`, `net_total`.
4. Strategy quyết định: cancel (when mispriced / risk exceeded) / place new orders.
5. Khi gửi lệnh: write-through local store (optimistic insert), gửi request async, chờ exchange confirm và reconcile tiếp.
6. Lặp lại.

**6. Quản lý pending orders**
- Lấy danh sách orders `Open` hoặc `PartiallyFilled` để làm nguồn `pending`.
- Dùng để: cancel when fair price moved, reduce risk when inventory lệch, enforce one-order-per-side.

**7. Exchange adapter (interface)**
Define a trait `Exchange` with async methods:
- `async fn fetch_open_orders(&self) -> Result<Vec<Order>>`
- `async fn fetch_order(&self, id: &str) -> Result<Option<Order>>`
- `async fn place_order(&self, req: PlaceOrderRequest) -> Result<PlacedOrder>`
- `async fn cancel_order(&self, id: &str) -> Result<CancelResult>`
- `async fn fetch_orderbook(&self, symbol: &str) -> Result<OrderBook>`

Implementations: `binance`, `okx`, `mock` (for unit tests). Keep adapters thin — convert exchange-specific types -> domain `Order`.

**8. Concurrency model & ownership (Rust patterns)**
- Runtime: `tokio` async.
- Engine core: single-threaded actor (task) that owns in-memory state (`HashMap<OrderId, Order>`, `Inventory`). External components (strategy, exchange adapters) send commands via `mpsc` channels.
- Use `Arc` + `RwLock` sparingly for read-only views (metrics, dashboards). Prefer message-passing to avoid races.
- Persistence writes via a dedicated task to serialize/apply WAL.

**9. Persistence & durability**
- Local store responsibilities: survive restart, allow reconciliation (replay pending). Recommended options: `sled` (embedded K/V), `sqlite` (with `rusqlite`), hoặc lightweight append-only WAL + snapshot files (serde + bincode/JSON).
- Store order records and checkpoints (snapshot of inventory). On startup: replay WAL and re-fetch exchange orders to rebuild canonical state.
- Version all serialized types and keep migration helpers.

**10. Risk checks & invariants**
- Invariants to enforce everywhere:
  - `remaining_size = size - size_filled` (>= 0)
  - `status == Filled` ⇔ `remaining_size == 0`
  - `|net_total| <= inventory_limit`
  - No two active orders on same side within `tick_tolerance` unless allowed.

**11. PnL / accounting (optional)**
- Keep fill ledger: record each fill (side, price, size, timestamp). Implement matching engine offline to compute realized PnL. Unmatched fills remain as inventory.

**12. Observability & metrics**
- Instrument: orders/sec, cancels/sec, fills/sec, net_position, net_pending, latency to place/cancel.
- Logging: structured logs (JSON) with `order_id`, `client_id`, `event`, `reason`.

**13. Testing strategy**
- Unit tests: models (arithmetic, invariants), state machine transitions.
- Integration tests: `mock` exchange implementing `Exchange` trait to simulate fills, partials, cancels.
- End-to-end: replay historical fills / orderbooks to validate behavior.

**14. Suggested Rust workspace layout**
- `hft_core` (domain types, inventory, order state, risk checks)
- `hft_engine` (reconciliation loop, actor, persistence)
- `hft_exchange_*` (adapters per exchange)
- `hft_strategy` (strategy algorithms, config-driven)
- `hft_tools` (metrics, logging, cli)

**15. Example types (concise)**
- `pub struct OrderId(String)`
- `pub enum Side { Buy, Sell }`
- `pub enum OrderStatus { Open, PartiallyFilled, Filled, Cancelled }`

**16. Lộ trình refactor (practical steps)**
1. Extract domain models (`Order`, `Inventory`, `OrderStatus`) và unit-test chúng.
2. Implement `Exchange` trait với `mock` adapter; write integration tests using mock.
3. Build in-memory engine (single-task actor) implementing reconciliation loop.
4. Add persistence (WAL + snapshot) and startup replay.
5. Implement a single real exchange adapter and run in staging with small size.
6. Add observability, harden error handling, then migrate strategies one-by-one.

**17. Operational notes**
- Always treat exchange as source-of-truth; local store is a cache/optimization and recovery aid.
- Keep idempotency keys (`client_id`) for place/cancel operations to safely retry.
- Configurable parameters: `inventory_limit`, `tick_tolerance`, `max_orders_per_side`, polling intervals.

**18. Invariants checklist for PRs**
- Include unit tests for arithmetic invariants.
- Include integration tests against `mock` showing lifecycle: place → partial fill → cancel.

---
File created to guide Rust refactor: [hft_live/architecture.md](hft_live/architecture.md)
