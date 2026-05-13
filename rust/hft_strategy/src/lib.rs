use rust_decimal::prelude::*;
use chrono::{DateTime, Utc};
use std::cmp::min;

pub const TICK_SIZE: f64 = 0.0001;
pub const MAX_GRID_LEVELS: usize = 128;

fn snap_down(price: f64, tick_size: f64) -> f64 {
    if price <= 0.0 {
        return 0.0;
    }
    let ticks = (price / tick_size + 1e-12).floor();
    (ticks * tick_size).max(0.0)
}

fn snap_up(price: f64, tick_size: f64) -> f64 {
    if price <= 0.0 {
        return 0.0;
    }
    let ticks = (price / tick_size - 1e-12).ceil();
    (ticks * tick_size).max(0.0)
}

#[derive(Debug, Clone)]
pub struct LiveGridEngine {
    pub fee: f64,
    pub max_position: f64,
    pub half_spread: f64,
    pub price_range: f64,
    pub grid_num: usize,
    pub update_threshold_price: f64,

    pub dollar_qty: f64,

    // state
    pub running_qty: f64,
    pub static_equity: f64,
    pub fee_paid: f64,
    pub fills: u64,

    pub prev_mid: Option<f64>,
    pub initialized: bool,

    pub current_nb: usize,
    pub current_na: usize,
    pub bid_prices: [f64; MAX_GRID_LEVELS],
    pub ask_prices: [f64; MAX_GRID_LEVELS],

    pub soft_cap: f64,
}

impl LiveGridEngine {
    pub fn new(
        fee: f64,
        max_position: f64,
        half_spread: f64,
        price_range: f64,
        grid_num: usize,
        update_threshold_ticks: i64,
        dollar_qty: f64,
    ) -> Self {
        Self {
            fee,
            max_position,
            half_spread,
            price_range,
            grid_num: grid_num.min(MAX_GRID_LEVELS),
            update_threshold_price: (update_threshold_ticks as f64) * TICK_SIZE,
            dollar_qty,
            running_qty: 0.0,
            static_equity: 0.0,
            fee_paid: 0.0,
            fills: 0,
            prev_mid: None,
            initialized: false,
            current_nb: 0,
            current_na: 0,
            bid_prices: [0.0; MAX_GRID_LEVELS],
            ask_prices: [0.0; MAX_GRID_LEVELS],
            soft_cap: 0.6,
        }
    }

    fn rebuild_grid(&mut self, mid: f64, best_bid: f64, best_ask: f64, _vol_factor: f64) {
        let pos_value = self.running_qty * mid;
        let denom = if self.max_position != 0.0 { self.max_position } else { 1.0 };
        let x = pos_value / denom;

        let mut nb = self.grid_num as isize;
        let mut na = self.grid_num as isize;

        if x >= 1.0 {
            nb = 0;
        } else if x <= -1.0 {
            na = 0;
        } else {
            if x > self.soft_cap {
                let mut ratio = (1.0 - x) / (1.0 - self.soft_cap);
                if ratio < 0.0 { ratio = 0.0; }
                nb = (self.grid_num as f64 * ratio).floor() as isize;
            } else if x < -self.soft_cap {
                let mut ratio = (1.0 + x) / (1.0 - self.soft_cap);
                if ratio < 0.0 { ratio = 0.0; }
                na = (self.grid_num as f64 * ratio).floor() as isize;
            }
        }

        if nb < 0 { nb = 0; }
        if na < 0 { na = 0; }

        self.current_nb = nb as usize;
        self.current_na = na as usize;

        let step = if self.grid_num > 1 {
            let mut s = self.price_range / ((self.grid_num - 1) as f64);
            if s < 0.0 { s = 0.0; }
            s
        } else { 0.0 };

        for j in 0..self.current_nb {
            let offset = self.half_spread + (j as f64) * step;
            let mut raw_price = mid - offset;
            if raw_price > best_bid { raw_price = best_bid; }
            let bid_price = snap_down(raw_price, TICK_SIZE);
            self.bid_prices[j] = if bid_price <= 0.0 { 0.0 } else { bid_price };
        }

        for j in 0..self.current_na {
            let offset = self.half_spread + (j as f64) * step;
            let mut raw_price = mid + offset;
            if raw_price < best_ask { raw_price = best_ask; }
            let ask_price = snap_up(raw_price, TICK_SIZE);
            self.ask_prices[j] = if ask_price <= 0.0 { 0.0 } else { ask_price };
        }

        for j in self.current_nb..MAX_GRID_LEVELS { self.bid_prices[j] = 0.0; }
        for j in self.current_na..MAX_GRID_LEVELS { self.ask_prices[j] = 0.0; }

        self.prev_mid = Some(mid);
        self.initialized = true;
    }

    pub fn step(&mut self, best_bid: f64, best_ask: f64, vol_factor: f64) -> Option<serde_json::Value> {
        if best_bid <= 0.0 || best_ask <= 0.0 { return None; }

        let mid = 0.5 * (best_bid + best_ask);
        let mut need_update = false;
        if !self.initialized || self.update_threshold_price <= 0.0 { need_update = true; }
        else {
            if let Some(prev) = self.prev_mid {
                if (mid - prev).abs() >= self.update_threshold_price { need_update = true; }
            } else { need_update = true; }
        }

        if need_update { self.rebuild_grid(mid, best_bid, best_ask, vol_factor); }

        let high = best_ask;
        let low = best_bid;

        if self.current_nb > 0 {
            for j in 0..self.current_nb {
                let bp = self.bid_prices[j];
                if bp <= 0.0 { continue; }
                if low <= bp {
                    let order_qty = 1.0;
                    self.running_qty += order_qty;
                    self.static_equity -= bp * order_qty;
                    self.fee_paid += bp * order_qty * self.fee;
                    self.fills += 1;
                }
            }
        }

        if self.current_na > 0 {
            for j in 0..self.current_na {
                let ap = self.ask_prices[j];
                if ap <= 0.0 { continue; }
                if high >= ap {
                    let order_qty = 1.0;
                    self.running_qty -= order_qty;
                    self.static_equity += ap * order_qty;
                    self.fee_paid += ap * order_qty * self.fee;
                    self.fills += 1;
                }
            }
        }

        let equity = self.static_equity + self.running_qty * mid - self.fee_paid;

        let mut bid_quotes = vec![];
        for j in 0..self.current_nb {
            let p = self.bid_prices[j];
            if p > 0.0 { bid_quotes.push((j, p, 1usize)); }
        }

        let mut ask_quotes = vec![];
        for j in 0..self.current_na {
            let p = self.ask_prices[j];
            if p > 0.0 { ask_quotes.push((j, p, 1usize)); }
        }

        // Return a JSON-like structure for simplicity
        let res = serde_json::json!({
            "mid": mid,
            "vol_factor": vol_factor,
            "running_qty": self.running_qty,
            "equity": equity,
            "fills": self.fills,
            "nb": self.current_nb,
            "na": self.current_na,
            "bid_quotes": bid_quotes,
            "ask_quotes": ask_quotes,
        });

        Some(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snap_rounding() {
        assert_eq!(snap_down(1.234567, 0.0001), 1.2345);
        assert_eq!(snap_up(1.234501, 0.0001), 1.2346);
    }
}
