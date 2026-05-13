use std::sync::Arc;
use async_trait::async_trait;
use anyhow::Result;
use rand::Rng;

use hft_core::{Order, OrderId, OrderStatus, PlaceOrderRequest, PlacedOrder, CancelResult, OrderBook, Side, Exchange};

pub struct MockExchange {
    // simple state could be added here
}

impl MockExchange {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Exchange for MockExchange {
    async fn fetch_open_orders(&self, _symbol: &str) -> Result<Vec<Order>> {
        Ok(vec![])
    }

    async fn fetch_order(&self, _symbol: &str, _id: &OrderId) -> Result<Option<Order>> {
        Ok(None)
    }

    async fn place_order(&self, _symbol: &str, req: PlaceOrderRequest) -> Result<PlacedOrder> {
        let mut rng = rand::thread_rng();
        let id = format!("mock-{}", rng.gen::<u64>());
        Ok(PlacedOrder { id: OrderId(id) })
    }

    async fn cancel_order(&self, _symbol: &str, _id: &OrderId) -> Result<CancelResult> {
        Ok(CancelResult { success: true })
    }

    async fn fetch_orderbook(&self, _symbol: &str) -> Result<OrderBook> {
        Ok(OrderBook {})
    }
}

pub type SharedMock = Arc<MockExchange>;
