//! Enchu Extend — PG透過キャッシュ。読み込み特化。

mod engine;

use std::sync::Mutex;

use napi::bindgen_prelude::*;
use napi_derive::napi;

use engine::{Engine, HimoType};

struct ExtendState {
    engine: Engine,
}

static STATE: Mutex<Option<ExtendState>> = Mutex::new(None);

fn with_engine<F, R>(f: F) -> Result<R>
where
    F: FnOnce(&Engine) -> Result<R>,
{
    let guard = STATE.lock().map_err(|e| Error::from_reason(format!("lock: {e}")))?;
    let state = guard.as_ref().ok_or_else(|| Error::from_reason("not initialized"))?;
    f(&state.engine)
}

fn with_engine_mut<F, R>(f: F) -> Result<R>
where
    F: FnOnce(&mut Engine) -> Result<R>,
{
    let mut guard = STATE.lock().map_err(|e| Error::from_reason(format!("lock: {e}")))?;
    let state = guard.as_mut().ok_or_else(|| Error::from_reason("not initialized"))?;
    f(&mut state.engine)
}

// ════════════════ Init ════════════════

#[napi]
pub fn init(path: String, _pg_conn: String, max_entities: Option<u32>) -> Result<()> {
    let engine = match max_entities {
        Some(max) => Engine::create_with_capacity(&path, max),
        None => Engine::create(&path),
    }.map_err(|e| Error::from_reason(format!("engine create: {e}")))?;
    let mut guard = STATE.lock().map_err(|e| Error::from_reason(format!("lock: {e}")))?;
    *guard = Some(ExtendState { engine });
    Ok(())
}

#[napi]
pub fn open(path: String, _pg_conn: String) -> Result<()> {
    let engine = Engine::open(&path)
        .map_err(|e| Error::from_reason(format!("engine open: {e}")))?;
    let mut guard = STATE.lock().map_err(|e| Error::from_reason(format!("lock: {e}")))?;
    *guard = Some(ExtendState { engine });
    Ok(())
}

#[napi]
pub fn close() -> Result<()> {
    let mut guard = STATE.lock().map_err(|e| Error::from_reason(format!("lock: {e}")))?;
    *guard = None;
    Ok(())
}

// ════════════════ 紐定義 ════════════════

#[napi]
pub fn define_himo(himo: String, himo_type: String, max_values: u32) -> Result<()> {
    with_engine_mut(|eng| {
        let ht = match himo_type.as_str() {
            "value" | "int" | "integer" => HimoType::Value,
            _ => HimoType::Symbol,
        };
        eng.define_himo(&himo, ht, max_values);
        Ok(())
    })
}

// ════════════════ Entity ════════════════

#[napi]
pub fn entity() -> Result<u32> {
    with_engine(|eng| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| eng.entity()));
        match result {
            Ok(eid) => Ok(eid),
            Err(_) => Err(Error::from_reason("entity limit reached")),
        }
    })
}

#[napi]
pub fn entity_count() -> Result<u32> {
    with_engine(|eng| Ok(eng.entity_count()))
}

// ════════════════ Write (同期用) ════════════════

#[napi]
pub fn tie_text(entity_id: u32, himo: String, value: String) -> Result<()> {
    with_engine_mut(|eng| { eng.tie_text(entity_id, &himo, &value); Ok(()) })
}

#[napi]
pub fn tie(entity_id: u32, himo: String, value: u32) -> Result<()> {
    with_engine_mut(|eng| { eng.tie(entity_id, &himo, value); Ok(()) })
}

// ════════════════ Read ════════════════

#[napi]
pub fn get_text(entity_id: u32, himo: String) -> Result<Option<String>> {
    with_engine(|eng| {
        Ok(eng.get_text(entity_id, &himo).map(|b| String::from_utf8_lossy(b).into_owned()))
    })
}

#[napi]
pub fn get_value(entity_id: u32, himo: String) -> Result<Option<u32>> {
    with_engine(|eng| Ok(eng.get(entity_id, &himo)))
}

/// バッチ読み出し（バイナリ）。
#[napi]
pub fn batch_get(entity_ids: Buffer, table: String, fields: Vec<String>) -> Result<Buffer> {
    with_engine(|eng| {
        let ids = unsafe {
            std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4)
        };

        let resolved: Vec<(usize, bool)> = fields.iter().map(|field_def| {
            let (fname, ftype) = field_def.split_once(':').unwrap_or((field_def, "text"));
            let himo = format!("{}.{}", table, fname);
            let hid = eng.himo_names().iter().position(|n| n == &himo).unwrap_or(usize::MAX);
            let is_int = ftype == "int" || ftype == "value";
            (hid, is_int)
        }).collect();

        let himos = eng.himo_store_slice();
        let vocab = eng.vocab_ref();
        let mut out: Vec<u8> = Vec::with_capacity(ids.len() * fields.len() * 8);

        for &eid in ids {
            for &(hid, is_int) in &resolved {
                if hid == usize::MAX {
                    out.extend_from_slice(&u32::MAX.to_le_bytes());
                    continue;
                }
                if is_int {
                    match himos[hid].get_value(eid) {
                        Some(v) => out.extend_from_slice(&v.to_le_bytes()),
                        None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
                    }
                } else {
                    match himos[hid].get_value(eid) {
                        Some(vid) => {
                            let b = vocab.get(vid);
                            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                            out.extend_from_slice(b);
                        }
                        None => out.extend_from_slice(&u32::MAX.to_le_bytes()),
                    }
                }
            }
        }
        Ok(Buffer::from(out))
    })
}

// ════════════════ Query ════════════════

#[napi]
pub fn rebuild() -> Result<()> {
    with_engine_mut(|eng| {
        eng.rebuild();
        #[cfg(feature = "v26")]
        eng.rebuild_pairs();
        Ok(())
    })
}

#[napi]
pub fn pull_raw(himo: String, value: u32) -> Result<Buffer> {
    with_engine(|eng| {
        let result = eng.pull_raw(&himo, value);
        let bytes = unsafe {
            std::slice::from_raw_parts(result.as_ptr() as *const u8, result.len() * 4)
        };
        Ok(Buffer::from(bytes))
    })
}

#[napi]
pub fn resolve_himo(himo: String) -> Result<u32> {
    with_engine(|eng| {
        let names = eng.himo_names();
        for (i, name) in names.iter().enumerate() {
            if name == &himo { return Ok(i as u32); }
        }
        Err(Error::from_reason(format!("unknown himo: {himo}")))
    })
}

#[napi]
pub fn resolve_field(table: String, field: String) -> Result<u32> {
    resolve_himo(format!("{table}.{field}"))
}

#[napi]
pub fn query_fast(table_id: u32, conditions: Buffer) -> Result<Buffer> {
    let _ = table_id;
    with_engine(|eng| {
        let cond_slice = unsafe {
            std::slice::from_raw_parts(conditions.as_ptr() as *const u32, conditions.len() / 4)
        };
        let names = eng.himo_names();
        let mut pairs: Vec<(&str, u32)> = Vec::with_capacity(cond_slice.len() / 2);
        for chunk in cond_slice.chunks_exact(2) {
            pairs.push((names[chunk[0] as usize].as_str(), chunk[1]));
        }
        let result = eng.query(&pairs);
        let bytes = unsafe {
            std::slice::from_raw_parts(result.as_ptr() as *const u8, result.len() * 4)
        };
        Ok(Buffer::from(bytes))
    })
}

#[napi]
pub fn query_count(table_id: u32, conditions: Buffer) -> Result<u32> {
    let _ = table_id;
    with_engine(|eng| {
        let cond_slice = unsafe {
            std::slice::from_raw_parts(conditions.as_ptr() as *const u32, conditions.len() / 4)
        };
        let names = eng.himo_names();
        let mut pairs: Vec<(&str, u32)> = Vec::with_capacity(cond_slice.len() / 2);
        for chunk in cond_slice.chunks_exact(2) {
            pairs.push((names[chunk[0] as usize].as_str(), chunk[1]));
        }
        Ok(eng.query(&pairs).len() as u32)
    })
}

#[napi]
pub fn vocab_lookup(value: String) -> Result<Option<u32>> {
    with_engine(|eng| Ok(eng.vocab_id(&value)))
}

/// 指定 entity 群の紐値を合計。entity_ids = Buffer<u32[]>
#[napi]
pub fn sum(himo: String, entity_ids: Buffer) -> Result<f64> {
    with_engine(|eng| {
        let ids = unsafe {
            std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4)
        };
        Ok(eng.sum(&himo, ids) as f64)
    })
}

/// GROUP BY + SUM。group_himo でグループ化し sum_himo を合計。
/// 返却: Buffer<[group_id, sum_hi, sum_lo, ...]> (各12バイト)
#[napi]
pub fn group_sum(group_himo: String, sum_himo: String, entity_ids: Buffer) -> Result<Buffer> {
    with_engine(|eng| {
        let ids = unsafe {
            std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4)
        };
        let result = eng.group_sum(&group_himo, &sum_himo, ids);
        let mut out = Vec::with_capacity(result.len() * 12);
        for (group, total) in &result {
            out.extend_from_slice(&group.to_le_bytes());
            out.extend_from_slice(&total.to_le_bytes());
        }
        Ok(Buffer::from(out))
    })
}

#[napi]
pub fn agg_min(himo: String, entity_ids: Buffer) -> Result<Option<u32>> {
    with_engine(|eng| {
        let ids = unsafe { std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4) };
        Ok(eng.min(&himo, ids))
    })
}

#[napi]
pub fn agg_max(himo: String, entity_ids: Buffer) -> Result<Option<u32>> {
    with_engine(|eng| {
        let ids = unsafe { std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4) };
        Ok(eng.max(&himo, ids))
    })
}

#[napi]
pub fn agg_avg(himo: String, entity_ids: Buffer) -> Result<Option<f64>> {
    with_engine(|eng| {
        let ids = unsafe { std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4) };
        Ok(eng.avg(&himo, ids).map(|v| v as f64))
    })
}

#[napi]
pub fn agg_count(himo: String, entity_ids: Buffer) -> Result<u32> {
    with_engine(|eng| {
        let ids = unsafe { std::slice::from_raw_parts(entity_ids.as_ptr() as *const u32, entity_ids.len() / 4) };
        Ok(eng.agg_count(&himo, ids))
    })
}

/// 範囲クエリ。min..=max の値を持つ entity を返す。
#[napi]
pub fn pull_range(himo: String, min: u32, max: u32) -> Result<Buffer> {
    with_engine(|eng| {
        let result = eng.pull_range(&himo, min, max);
        let bytes = unsafe {
            std::slice::from_raw_parts(result.as_ptr() as *const u8, result.len() * 4)
        };
        Ok(Buffer::from(bytes))
    })
}

/// 日付範囲クエリ。from/to は [year, month, day]。
#[napi]
pub fn pull_date_range(himo: String, from: Vec<u32>, to: Vec<u32>) -> Result<Buffer> {
    with_engine(|eng| {
        let result = eng.pull_date_range(&himo, (from[0], from[1], from[2]), (to[0], to[1], to[2]));
        let bytes = unsafe {
            std::slice::from_raw_parts(result.as_ptr() as *const u8, result.len() * 4)
        };
        Ok(Buffer::from(bytes))
    })
}

#[napi]
pub fn flush() -> Result<()> {
    with_engine_mut(|eng| {
        eng.flush().map_err(|e| Error::from_reason(format!("flush: {e}")))
    })
}
