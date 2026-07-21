//! Tiny NDJSON external-transform fixture worker for sync-transform tests.
//!
//! Wire protocol (stdin/stdout):
//! - Request:  `{"batch_id", "count"}\n` + `count` item lines
//! - Response: `{"batch_id", "count"}\n` + item lines (must echo batch_id)
//!
//! Modes (first CLI arg):
//! - `echo` — echo items unchanged
//! - `mutate` — set `data.name` / `fields.name` text to `"mutated"`
//! - `bad-batch-id` — respond with batch_id+1 (for mismatch tests)
//! - `missing-batch-id` — respond with a header lacking batch_id
//! - `error-once` — first response is a framed `error`; later responses echo

use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

fn main() {
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "echo".to_string());
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut lines = stdin.lock().lines();
    let exchanges = AtomicUsize::new(0);

    while let Some(Ok(header_line)) = lines.next() {
        if header_line.trim().is_empty() {
            continue;
        }
        let header: Value = match serde_json::from_str(&header_line) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("fixture_worker: bad header: {e}");
                std::process::exit(2);
            }
        };
        let batch_id = header.get("batch_id").and_then(|v| v.as_u64()).unwrap_or(0);
        let count = header.get("count").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            let Some(Ok(line)) = lines.next() else {
                eprintln!("fixture_worker: unexpected EOF reading items");
                std::process::exit(2);
            };
            let mut item: Value = serde_json::from_str(&line).unwrap_or_else(|e| {
                eprintln!("fixture_worker: bad item JSON: {e}");
                std::process::exit(2);
            });
            if mode == "mutate" {
                mutate_name(&mut item);
            }
            items.push(item);
        }

        let n = exchanges.fetch_add(1, Ordering::SeqCst);
        let out_header = match mode.as_str() {
            "bad-batch-id" => json!({"batch_id": batch_id.saturating_add(1), "count": items.len()}),
            "missing-batch-id" => json!({"count": items.len()}),
            "error-once" if n == 0 => {
                json!({"batch_id": batch_id, "error": "fixture deliberate error"})
            }
            _ => json!({"batch_id": batch_id, "count": items.len()}),
        };

        let header_str = serde_json::to_string(&out_header).expect("header");
        writeln!(stdout, "{header_str}").expect("write header");
        if out_header.get("error").is_none() {
            for item in items {
                let s = serde_json::to_string(&item).expect("item");
                writeln!(stdout, "{s}").expect("write item");
            }
        }
        stdout.flush().expect("flush");
    }
}

fn mutate_name(item: &mut Value) {
    // UniversalChange: data.name; UniversalRow / UniversalRelation: fields/data.name
    // UniversalRelationChange: relation.data.name
    mutate_name_in_maps(item);
    if let Some(relation) = item.get_mut("relation") {
        mutate_name_in_maps(relation);
    }
}

fn mutate_name_in_maps(item: &mut Value) {
    // UniversalValue tagged: {"type":"VarChar","value":{"value":"...","length":N}}
    //                    or: {"type":"Text","value":"..."}
    for key in ["data", "fields"] {
        let Some(obj) = item.get_mut(key).and_then(|v| v.as_object_mut()) else {
            continue;
        };
        let Some(name) = obj.get_mut("name").and_then(|v| v.as_object_mut()) else {
            continue;
        };
        match name.get("type").and_then(|t| t.as_str()) {
            Some("VarChar") => {
                if let Some(inner) = name.get_mut("value").and_then(|v| v.as_object_mut()) {
                    inner.insert("value".into(), json!("mutated"));
                }
            }
            Some("Text") => {
                name.insert("value".into(), json!("mutated"));
            }
            _ => {}
        }
    }
}
