use std::sync::Arc;
use tokio::time::{sleep, Duration};
use anyhow::Result;
use rust_decimal::Decimal;

use hft_core::Inventory;
use hft_exchange_mock::MockExchange;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let exchange = Arc::new(MockExchange::new());
    let symbol = "BTCUSDT";

    // simple loop: fetch open orders and compute inventory placeholder
    loop {
        let orders = exchange.fetch_open_orders(symbol).await?;
        let mut inv = Inventory::default();
        // in real code we would aggregate fills and pending; placeholder
        println!("fetched {} open orders", orders.len());
        println!("inventory net_filled={} net_pending={} net_total={}",
            inv.net_filled(), inv.net_pending(), inv.net_total());
        sleep(Duration::from_secs(1)).await;
        break; // run one loop for example
    }

    Ok(())
}
