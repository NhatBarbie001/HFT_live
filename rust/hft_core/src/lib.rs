use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json;
use sled;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct OrderId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: OrderId,
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub size_filled: Decimal,
    pub remaining_size: Decimal,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Order {
    pub fn recompute_remaining(&mut self) {
        let rem = self.size - self.size_filled;
        if rem.is_sign_negative() {
            self.remaining_size = Decimal::ZERO;
        } else {
            self.remaining_size = rem;
        }
        if self.remaining_size == Decimal::ZERO {
            self.status = OrderStatus::Filled;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Inventory {
    pub filled_buy: Decimal,
    pub filled_sell: Decimal,
    pub pending_buy: Decimal,
    pub pending_sell: Decimal,
}

impl Inventory {
    pub fn net_filled(&self) -> Decimal {
        self.filled_buy - self.filled_sell
    }
    pub fn net_pending(&self) -> Decimal {
        self.pending_buy - self.pending_sell
    }
    pub fn net_total(&self) -> Decimal {
        self.net_filled() + self.net_pending()
    }
}

// Simple persistence using sled for position store
pub struct PositionStore {
    db: sled::Db,
}

impl PositionStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let db = sled::open(path)?;
        Ok(Self { db })
    }

    /// Save an inventory snapshot for `symbol` as JSON
    pub fn save_inventory(&self, symbol: &str, inv: &Inventory) -> anyhow::Result<()> {
        let key = format!("inv:{}", symbol);
        let v = serde_json::to_vec(inv)?;
        self.db.insert(key.as_bytes(), v)?;
        self.db.flush()?;
        Ok(())
    }

    /// Load inventory snapshot for `symbol`
    pub fn load_inventory(&self, symbol: &str) -> anyhow::Result<Option<Inventory>> {
        let key = format!("inv:{}", symbol);
        if let Some(v) = self.db.get(key.as_bytes())? {
            let inv: Inventory = serde_json::from_slice(&v)?;
            Ok(Some(inv))
        } else {
            Ok(None)
        }
    }
}

// Exchange trait to be implemented by adapters.
#[derive(Debug, Clone)]
pub struct PlaceOrderRequest {
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone)]
pub struct PlacedOrder {
    pub id: OrderId,
}

#[derive(Debug, Clone)]
pub struct CancelResult {
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    // minimal placeholder
}

#[async_trait]
pub trait Exchange: Send + Sync {
    async fn fetch_open_orders(&self, symbol: &str) -> anyhow::Result<Vec<Order>>;
    async fn fetch_order(&self, symbol: &str, id: &OrderId) -> anyhow::Result<Option<Order>>;
    async fn place_order(&self, symbol: &str, req: PlaceOrderRequest) -> anyhow::Result<PlacedOrder>;
    async fn cancel_order(&self, symbol: &str, id: &OrderId) -> anyhow::Result<CancelResult>;
    async fn fetch_orderbook(&self, symbol: &str) -> anyhow::Result<OrderBook>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use chrono::Utc;

    #[test]
    fn test_order_recompute() {
        let mut o = Order {
            id: OrderId("1".into()),
            client_id: None,
            side: Side::Buy,
            price: Decimal::new(10000, 2),
            size: Decimal::new(5, 0),
            size_filled: Decimal::new(2, 0),
            remaining_size: Decimal::new(0, 0),
            status: OrderStatus::Open,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        o.recompute_remaining();
        assert_eq!(o.remaining_size, Decimal::new(3, 0));
    }
}
