use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use anyhow::Result;
use env_logger;
use rand::Rng;

use hft_strategy::LiveGridEngine;
use hft_exchange_mock::MockExchange;
use hft_core::PositionStore;
use hft_core::Inventory;
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // Simple demo: generate ticks and run engine one round
    let mut engine = LiveGridEngine::new(
        0.0002,
        100.0,
        0.0002,
        0.0052,
        14,
        1,
        10.0,
    );

    let exchange = Arc::new(MockExchange::new());

    // Simulate a small series of ticks
    let mut rng = rand::thread_rng();
    for i in 0..5 {
        let base = 1.0 + (i as f64) * 0.0001;
        let best_bid = base - 0.00005 * rng.gen::<f64>();
        let best_ask = base + 0.00005 * rng.gen::<f64>();
        if let Some(res) = engine.step(best_bid, best_ask, 1.0) {
            println!("tick {}: {}", i, res);
            // Persist a simple inventory snapshot (demo)
            let store = PositionStore::open("./positions_db")?;
            let inv = Inventory {
                filled_buy: Decimal::from_f64(engine.running_qty.max(0.0)).unwrap_or(Decimal::ZERO),
                filled_sell: Decimal::from_f64((-engine.running_qty).max(0.0)).unwrap_or(Decimal::ZERO),
                pending_buy: Decimal::ZERO,
                pending_sell: Decimal::ZERO,
            };
            store.save_inventory("demo_symbol", &inv)?;
        }
        sleep(Duration::from_millis(200)).await;
    }

    // Example place order via mock exchange
    let req = hft_core::PlaceOrderRequest {
        client_id: Some("demo".into()),
        side: hft_core::Side::Buy,
        price: rust_decimal::Decimal::new(12345, 4),
        size: rust_decimal::Decimal::new(1, 0),
    };

    let placed = exchange.place_order("BTCUSDT", req).await?;
    println!("placed mock order id: {:?}", placed.id);

    Ok(())
}
