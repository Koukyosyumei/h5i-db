//! Build an h5i-db database for the demo: the instruments catalog, hourly
//! book snapshots from the synthetic market in `lab_server::market`, and a
//! seal over the last days of the book, which is the holdout.
//!
//! Usage: lab-demo-data <new database directory> [days] [sealed days]
//! Prints one JSON line: the data's start, the seal's start, and its end.

use h5i_db_core::Database;
use h5i_db_service::{SealCall, Service};
use lab_server::market::{DAY_NS, START_NS, demo_series, seed};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or("usage: lab-demo-data <dir> [days] [sealed days]")?;
    let days: u32 = args.next().map(|v| v.parse()).transpose()?.unwrap_or(180);
    let sealed: u32 = args.next().map(|v| v.parse()).transpose()?.unwrap_or(30);
    if sealed == 0 || sealed >= days {
        return Err("sealed days must be between 1 and days - 1".into());
    }
    let db = Database::create(path.as_ref()).await?;
    let series = demo_series();
    let end = seed(&db, &series, days, 20_251_016).await?;
    let service = Service::open(db).await?;
    let seal_start = end - i64::from(sealed) * DAY_NS;
    service
        .seal(&SealCall {
            idempotency_key: "demo-seal".into(),
            id: "holdout".into(),
            table: "book_deltas".into(),
            start_ns: seal_start,
            end_ns: end,
            note: Some(format!("last {sealed} days of the book")),
        })
        .await?;
    let ids: Vec<&str> = series.iter().map(|item| item.id).collect();
    println!(
        "{}",
        serde_json::json!({
            "start_ns": START_NS,
            "seal_start_ns": seal_start,
            "end_ns": end,
            "instruments": ids,
        })
    );
    Ok(())
}
