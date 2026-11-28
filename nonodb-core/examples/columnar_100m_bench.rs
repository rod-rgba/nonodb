//! Large columnar collection smoke benchmark.
//!
//! Usage:
//!   cargo run --release -p nonodb-core --example columnar_100m_bench -- <data-dir> <collection>

use std::time::{Duration, Instant};

use nonodb_core::{columnar, Error, FindQuery, Store};
use serde_json::{json, Value};

fn pct(values: &mut [Duration], p: f64) -> Duration {
    values.sort_unstable();
    let index = ((values.len() as f64 - 1.0) * p).round() as usize;
    values[index]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).ok_or("usage: <data-dir> <collection>")?;
    let name = args.get(2).map(String::as_str).unwrap_or("ohlc_data");
    let store = Store::open_with_map_size(dir, 64usize * 1024 * 1024 * 1024)?;
    let coll = store.collection(name)?;

    let txn = store.read_txn()?;
    let stats = coll.storage_stats(&txn)?;
    drop(txn);
    println!(
        "storage: rows={} data_bytes={} index_bytes={} indexes={} id_bytes={} logical_bytes={}",
        stats.records,
        stats.data_bytes,
        stats.index_bytes,
        stats.indexes,
        stats.id_bytes,
        stats.logical_bytes
    );

    let started = Instant::now();
    let count = coll.count(&json!({}))?;
    println!("count {{}}: {count} in {:?}", started.elapsed());

    // Use a bounded sample for point reads. The standard profile can answer a
    // limited find directly; columnar uses the streaming bucket iterator so it
    // does not materialize the whole collection.
    let profile = std::env::var("TARGET_PROFILE").unwrap_or_else(|_| "columnar".to_string());
    let sample = if profile == "standard" {
        coll.find(&FindQuery {
            filter: json!({}),
            limit: Some(10_000),
            ..Default::default()
        })?
    } else {
        let txn = store.read_txn()?;
        let mut sample = Vec::with_capacity(10_000);
        let _ = columnar::for_each(&txn, store.env(), name, |doc| {
            sample.push(doc);
            if sample.len() >= 10_000 {
                return Err(Error::Query("__sample_complete".into()));
            }
            Ok(())
        });
        drop(txn);
        sample
    };
    let ids: Vec<String> = sample
        .iter()
        .filter_map(|doc| {
            doc.get("_id")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .collect();
    let symbol = sample
        .iter()
        .find_map(|doc| {
            doc.get("symbol")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_default();
    println!(
        "streamed sample for point reads: {} ids, symbol={symbol}",
        ids.len()
    );

    let mut points = Vec::with_capacity(20_000);
    for index in 0..20_000 {
        let id = &ids[index % ids.len()];
        let started = Instant::now();
        let found = coll.find_by_key(id)?;
        points.push(started.elapsed());
        assert!(found.is_some(), "missing {id}");
    }
    println!(
        "point read p50={:?} p95={:?}",
        pct(&mut points, 0.50),
        pct(&mut points, 0.95)
    );

    let started = Instant::now();
    let groups = coll.aggregate(&[
        json!({"$group": {"_id": "$symbol", "n": {"$sum": 1}}}),
        json!({"$sort": {"n": -1}}),
        json!({"$limit": 20}),
    ])?;
    println!(
        "group by symbol: {} groups in {:?}",
        groups.len(),
        started.elapsed()
    );
    Ok(())
}
