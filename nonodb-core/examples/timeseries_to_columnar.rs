//! Stream an existing time-series collection into a columnar collection.
//!
//! Usage:
//!   cargo run --release -p nonodb-core --example timeseries_to_columnar -- \
//!     <source-dir> <source-coll> <target-dir> <target-coll> [limit] [batch]

use std::time::Instant;

use nonodb_core::columnar::Settings as ColumnarSettings;
use nonodb_core::timeseries;
use nonodb_core::{CollectionProfile, Store};
use serde_json::{json, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let source_dir = args
        .get(1)
        .ok_or("usage: <source-dir> <source-coll> <target-dir> <target-coll> [limit] [batch]")?;
    let source_coll = args.get(2).map(String::as_str).unwrap_or("ohlc_data");
    let target_dir = args.get(3).ok_or("missing target-dir")?;
    let target_coll = args.get(4).map(String::as_str).unwrap_or("ohlc_data");
    let limit: u64 = args
        .get(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000_000);
    let batch: usize = args.get(6).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let map_size = 64usize * 1024 * 1024 * 1024;
    let source = Store::open_with_map_size(source_dir, map_size)?;
    let source_settings = source
        .timeseries_settings(source_coll)?
        .ok_or_else(|| format!("{source_coll} is not a time-series collection"))?;

    let target = Store::open_with_map_size(target_dir, map_size)?;
    if target.collections()?.iter().any(|name| name == target_coll) {
        return Err(format!("target collection {target_coll} already exists").into());
    }
    let profile = std::env::var("TARGET_PROFILE").unwrap_or_else(|_| "columnar".to_string());
    if profile == "standard" {
        target.create_collection(target_coll, CollectionProfile::Standard)?;
    } else if profile == "columnar" {
        target.create_columnar_collection(target_coll, &ColumnarSettings::default())?;
    } else {
        return Err(format!("unknown TARGET_PROFILE={profile}").into());
    }
    let target_coll_ref = target.collection(target_coll)?;

    let rtxn = source.read_txn()?;
    let started = Instant::now();
    let mut seen = 0u64;
    let mut written = 0u64;
    let mut last_report = Instant::now();
    let mut first = true;

    timeseries::scan_batches(
        &rtxn,
        source.env(),
        source_coll,
        &source_settings,
        &json!({}),
        batch,
        |docs| {
            if first {
                if let Some(doc) = docs.first() {
                    eprintln!("source sample: {doc}");
                }
                first = false;
            }
            let mut prepared = Vec::with_capacity(docs.len());
            for mut doc in docs {
                let id = match doc.get("_id") {
                    Some(value) => nonodb_core::id::key_from_value(value)
                        .ok_or_else(|| nonodb_core::Error::InvalidId)?,
                    None => {
                        let id = format!("ohlc-{written:012}");
                        let object = doc.as_object_mut().ok_or_else(|| {
                            nonodb_core::Error::Query("document must be an object".into())
                        })?;
                        object.insert("_id".into(), Value::String(id.clone()));
                        id
                    }
                };
                let _ = id;
                prepared.push(doc);
            }
            let mut wtxn = target.write_txn_unsynced()?;
            target_coll_ref.insert_many_txn(&mut wtxn, &prepared)?;
            wtxn.commit_unsynced()?;
            seen += prepared.len() as u64;
            written = seen;
            if seen % 500_000 < batch as u64 {
                target.force_sync()?;
            }
            if last_report.elapsed().as_secs() >= 10 {
                let elapsed = started.elapsed().as_secs_f64();
                let stats = target
                    .read_txn()
                    .ok()
                    .and_then(|txn| target_coll_ref.storage_stats(&txn).ok());
                eprintln!(
                    "imported {seen} docs in {elapsed:.1}s ({:.0} docs/s){}",
                    seen as f64 / elapsed,
                    stats
                        .map(|s| format!(
                            ", target rows={} data_bytes={} index_bytes={}",
                            s.records, s.data_bytes, s.index_bytes
                        ))
                        .unwrap_or_default()
                );
                last_report = Instant::now();
            }
            Ok(seen < limit)
        },
    )?;

    target.force_sync()?;
    if std::env::var("COMPACT_AFTER").ok().as_deref() == Some("1") {
        let compact_started = Instant::now();
        let compacted = target.compact_columnar(target_coll)?;
        eprintln!(
            "compacted {compacted} rows in {:.1}s",
            compact_started.elapsed().as_secs_f64()
        );
    }
    if let Ok(path) = std::env::var("COPY_AFTER") {
        target.copy_to_file(
            std::path::Path::new(&path),
            nonodb_core::heed::CompactionOption::Enabled,
        )?;
        let bytes = std::fs::metadata(&path)?.len();
        eprintln!("copied compact database to {path}: {bytes} bytes");
    }
    let elapsed = started.elapsed().as_secs_f64();
    let txn = target.read_txn()?;
    let stats = target_coll_ref.storage_stats(&txn)?;
    println!(
        "done: {seen} docs in {elapsed:.1}s ({:.0} docs/s), target rows={} data_bytes={} index_bytes={}",
        seen as f64 / elapsed,
        stats.records,
        stats.data_bytes,
        stats.index_bytes
    );
    Ok(())
}
