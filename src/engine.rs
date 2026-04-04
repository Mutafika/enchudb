//! v23 Engine — 量子円柱完成版。Column+Cylinder+Undo。Reverseなし。
//!
//!   entity() → ID 振る
//!   tie_text → 文字列を紐で張る（Vocabulary 経由）
//!   tie     → u32 値を紐で張る
//!   untie → 紐を外す
//!   content/get_content → 非索引テキスト
//!   query → 円柱の重なりを一発で返す
//!   delete → entity 削除
//!   commit/rollback → トランザクション（undo ログ）
//!   open/flush → 永続化（mmap なので open は即利用可）

use std::collections::HashMap;
use std::io;

use crate::vocabulary::Vocabulary;
use crate::entity_set::EntitySet;
use crate::himo_store::{HimoStore, HimoType};
use crate::content_store::ContentStore;
use crate::undo::UndoLog;

// ════════════════ ギャロッピング交差 ════════════════

#[inline]
fn galloping_intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if small.is_empty() { return vec![]; }
    let mut result = Vec::with_capacity(small.len());
    let mut lo = 0usize;
    for &val in small {
        lo = gallop_ge(big, val, lo);
        if lo >= big.len() { break; }
        if big[lo] == val { result.push(val); lo += 1; }
    }
    result
}

#[inline]
fn gallop_ge(big: &[u32], val: u32, lo: usize) -> usize {
    let n = big.len();
    if lo >= n { return n; }
    if big[lo] >= val { return lo; }
    let mut step = 1usize;
    let mut hi = lo + step;
    while hi < n && big[hi] < val { step *= 2; hi = (lo + step).min(n); }
    let from = lo + step / 2;
    let to = hi.min(n);
    from + big[from..to].partition_point(|&x| x < val)
}

// ════════════════ Engine ════════════════

pub struct Engine {
    dir: String,
    max_entities: u32,
    vocab: Vocabulary,
    himo_reg: Vocabulary,
    himo_to_id: HashMap<String, usize>,
    himo_names: Vec<String>,
    himo_types: Vec<HimoType>,
    himo_max_values: Vec<u32>,
    himos: Vec<HimoStore>,
    entities: EntitySet,
    contents: ContentStore,
    undo: UndoLog,
}

const DEFAULT_MAX_ENTITIES: u32 = 16_777_216; // 16M

impl Engine {
    pub fn create(dir: &str) -> io::Result<Self> {
        Self::create_with_capacity(dir, DEFAULT_MAX_ENTITIES)
    }

    pub fn create_with_capacity(dir: &str, max_entities: u32) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let vocab = Vocabulary::create_with_params(&format!("{dir}/_vocab"), max_entities, 256, max_entities / 128)?;
        let himo_reg = Vocabulary::create(&format!("{dir}/_himoreg"))?;
        let entities = EntitySet::create_with(&format!("{dir}/_entities.dat"), max_entities)?;
        let contents = ContentStore::create(&format!("{dir}/_content"))?;
        let undo = UndoLog::create(&format!("{dir}/_undo.dat"))?;
        Ok(Self {
            dir: dir.to_string(), max_entities, vocab, himo_reg,
            himo_to_id: HashMap::new(), himo_names: Vec::new(),
            himo_types: Vec::new(), himo_max_values: Vec::new(),
            himos: Vec::new(), entities, contents, undo,
        })
    }

    pub fn open(dir: &str) -> io::Result<Self> {
        let vocab = Vocabulary::open(&format!("{dir}/_vocab"))?;
        let himo_reg = Vocabulary::open(&format!("{dir}/_himoreg"))?;
        let entities = EntitySet::open(&format!("{dir}/_entities.dat"))?;
        let contents = ContentStore::open(&format!("{dir}/_content"))?;

        // 紐を復元
        let mut himo_to_id = HashMap::new();
        let mut himo_names = Vec::new();
        let mut himo_types = Vec::new();
        let mut himo_max_values = Vec::new();
        let mut himos = Vec::new();

        let himo_types_path = format!("{dir}/_himotypes.bin");
        let type_bytes = if std::path::Path::new(&himo_types_path).exists() {
            std::fs::read(&himo_types_path)?
        } else { vec![] };

        let maxv_path = format!("{dir}/_himomaxv.bin");
        let maxv_bytes = if std::path::Path::new(&maxv_path).exists() {
            std::fs::read(&maxv_path)?
        } else { vec![] };

        let himo_count = type_bytes.len();
        for hid in 0..himo_count {
            let ht = HimoType::from_byte(type_bytes[hid]);
            let mv = if hid * 4 + 4 <= maxv_bytes.len() {
                u32::from_le_bytes(maxv_bytes[hid * 4..hid * 4 + 4].try_into().unwrap())
            } else { 0 };
            let name_bytes = himo_reg.get(hid as u32);
            let name = String::from_utf8_lossy(name_bytes).to_string();
            let himo_dir = format!("{dir}/_himos/{hid}");
            let hs = HimoStore::open(&himo_dir, ht)?;
            himo_to_id.insert(name.clone(), hid);
            himo_names.push(name);
            himo_types.push(ht);
            himo_max_values.push(mv);
            himos.push(hs);
        }

        let undo = UndoLog::open(&format!("{dir}/_undo.dat"))?;

        let mut eng = Self {
            dir: dir.to_string(), max_entities: DEFAULT_MAX_ENTITIES, vocab, himo_reg,
            himo_to_id, himo_names, himo_types, himo_max_values,
            himos, entities, contents, undo,
        };

        // クラッシュ復旧: 未コミットの変更を巻き戻す
        if eng.undo.pending_count() > 0 {
            eng.recover();
        }

        Ok(eng)
    }

    // ──── entity ────

    pub fn entity(&self) -> u32 {
        self.entities.allocate()
    }

    pub(crate) fn entities(&self) -> Vec<u32> { self.entities.iter() }
    pub(crate) fn entity_count(&self) -> usize { self.entities.count() as usize }

    // ──── tie ────

    /// 紐を登録。max_values > 0 なら prefix sum O(1)。
    pub fn define_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) {
        self.ensure_himo(himo, ht, max_values);
    }

    /// 紐を張る（未定義なら自動作成、max_values=0）。&mut self。
    pub fn tie_text(&mut self, eid: u32, himo: &str, value: &str) {
        let vid = self.vocab.get_or_insert(value.as_bytes());
        let hid = self.ensure_himo(himo, HimoType::Symbol, 0);
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, vid);
    }

    /// 紐を張る（未定義なら自動作成、max_values=0）。&mut self。
    pub fn tie(&mut self, eid: u32, himo: &str, value: u32) {
        let hid = self.ensure_himo(himo, HimoType::Value, 0);
        self.record_undo(eid, hid);
        self.himos[hid].set(eid, value);
    }

    // ──── untie ────

    pub fn untie(&self, eid: u32, himo: &str) {
        if let Some(&hid) = self.himo_to_id.get(himo) {
            self.record_undo(eid, hid);
            self.himos[hid].remove(eid);
        }
    }

    // ──── delete ────

    pub fn delete(&self, eid: u32) {
        for hid in 0..self.himos.len() {
            self.record_undo(eid, hid);
            self.himos[hid].remove(eid);
        }
        self.entities.free(eid);
    }

    // ──── トランザクション ────

    /// 変更を確定。undo をクリア。
    pub fn commit(&self) {
        self.undo.commit();
    }

    /// 変更を巻き戻す。
    pub fn rollback(&self) {
        for (eid, himo_id, old_value) in self.undo.entries_reverse() {
            let hid = himo_id as usize;
            if hid < self.himos.len() {
                self.himos[hid].restore(eid, &old_value);
            }
        }
        self.undo.commit();
    }

    fn record_undo(&self, eid: u32, hid: usize) {
        if hid < self.himos.len() {
            let old = self.himos[hid].get_raw_bytes(eid);
            self.undo.record(eid, hid as u16, &old);
        }
    }

    fn recover(&mut self) {
        for (eid, himo_id, old_value) in self.undo.entries_reverse() {
            let hid = himo_id as usize;
            if hid < self.himos.len() {
                self.himos[hid].restore(eid, &old_value);
            }
        }
        self.undo.commit();
    }

    // ──── content ────

    pub fn content(&self, eid: u32, key: &str, data: &[u8]) {
        self.contents.set(eid, key, data);
    }

    pub fn get_content(&self, eid: u32, key: &str) -> Option<&[u8]> {
        self.contents.get(eid, key)
    }

    // ──── get ────

    pub fn get_text(&self, eid: u32, himo: &str) -> Option<&[u8]> {
        let hid = *self.himo_to_id.get(himo)?;
        let vid = self.himos[hid].get_value(eid)?;
        Some(self.vocab.get(vid))
    }

    pub fn get(&self, eid: u32, himo: &str) -> Option<u32> {
        let hid = *self.himo_to_id.get(himo)?;
        self.himos[hid].get_value(eid)
    }

    pub fn vocab_id(&self, text: &str) -> Option<u32> { self.vocab.lookup(text.as_bytes()) }

    pub fn himos_of(&self, eid: u32) -> Vec<&str> {
        self.himos.iter().enumerate()
            .filter(|(_, ds)| ds.get_value(eid).is_some())
            .map(|(i, _)| self.himo_names[i].as_str())
            .collect()
    }
    pub(crate) fn vocab(&self) -> &Vocabulary { &self.vocab }
    pub fn himo_names(&self) -> &[String] { &self.himo_names }

    // ──── 紐を引く（Cylinder 経由）────

    /// 円柱を最新にする。書き込み後に呼ぶ。
    pub fn rebuild(&self) {
        for ds in &self.himos { ds.rebuild_cylinder(); }
    }

    /// 紐を引く。Cylinder のスナップショットから O(log n)。
    /// rebuild を呼ぶまで書き込み分は反映されない。
    pub fn pull_raw(&self, himo: &str, value: u32) -> &[u32] {
        match self.himo_to_id.get(himo) {
            Some(&idx) => self.himos[idx].cylinder().slice_one(value),
            None => &[],
        }
    }

    pub fn query(&self, strings: &[(&str, u32)]) -> Vec<u32> {
        self.rebuild();
        if strings.is_empty() { return vec![]; }
        if strings.len() == 1 {
            return match self.himo_to_id.get(strings[0].0) {
                Some(&idx) => self.himos[idx].cylinder().slice_one(strings[0].1).to_vec(),
                None => vec![],
            };
        }
        let mut slices: Vec<&[u32]> = Vec::with_capacity(strings.len());
        for &(himo, val) in strings {
            if let Some(&idx) = self.himo_to_id.get(himo) {
                let s = self.himos[idx].cylinder().slice_one(val);
                if s.is_empty() { return vec![]; }
                slices.push(s);
            }
        }
        if slices.len() != strings.len() { return vec![]; }
        slices.sort_by_key(|s| s.len());
        let mut result = galloping_intersect(slices[0], slices[1]);
        for s in &slices[2..] {
            if result.is_empty() { return vec![]; }
            result = galloping_intersect(&result, s);
        }
        result
    }

    pub(crate) fn query_count(&self, strings: &[(&str, u32)]) -> usize {
        self.query(strings).len()
    }

    // ──── himo 管理 ────

    fn ensure_himo(&mut self, himo: &str, ht: HimoType, max_values: u32) -> usize {
        if let Some(&idx) = self.himo_to_id.get(himo) { return idx; }
        let hid = self.himos.len();
        self.himo_reg.get_or_insert(himo.as_bytes());
        let himo_dir = format!("{}/_himos/{hid}", self.dir);
        let hs = HimoStore::create_with(&himo_dir, ht, max_values, self.max_entities).unwrap();
        self.himos.push(hs);
        self.himo_to_id.insert(himo.to_string(), hid);
        self.himo_names.push(himo.to_string());
        self.himo_types.push(ht);
        self.himo_max_values.push(max_values);
        hid
    }

    // ──── flush ────

    pub fn flush(&mut self) -> io::Result<()> {
        self.commit();
        for ds in &self.himos { ds.flush()?; }
        self.vocab.flush()?;
        self.himo_reg.flush()?;
        self.entities.flush()?;
        self.contents.flush()?;
        self.undo.flush()?;

        let type_bytes: Vec<u8> = self.himo_types.iter().map(|ht| *ht as u8).collect();
        std::fs::write(format!("{}/_himotypes.bin", self.dir), &type_bytes)?;

        let maxv_bytes: Vec<u8> = self.himo_max_values.iter().flat_map(|v| v.to_le_bytes()).collect();
        std::fs::write(format!("{}/_himomaxv.bin", self.dir), &maxv_bytes)?;

        Ok(())
    }
}

// ════════════════ テスト ════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let dir = format!("/tmp/enchu_v23_{name}");
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    // ──── entity ライフサイクル ────

    #[test]
    fn entity_create_and_count() {
        let dir = tmp("ent_create");
        let mut eng = Engine::create(&dir).unwrap();
        assert_eq!(eng.entity_count(), 0);
        let e0 = eng.entity();
        let e1 = eng.entity();
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.entities(), vec![e0, e1]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn entity_delete_and_reuse() {
        let dir = tmp("ent_del");
        let mut eng = Engine::create(&dir).unwrap();
        let e0 = eng.entity();
        let e1 = eng.entity();
        let e2 = eng.entity();

        eng.delete(e1);
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.entities(), vec![e0, e2]);

        let e3 = eng.entity();
        assert_eq!(e3, e1); // ID 再利用
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── tie / get 全型 ────

    #[test]
    fn tie_text_roundtrip() {
        let dir = tmp("tie_text");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "name", "田中");
        assert_eq!(eng.get_text(e, "name"), Some("田中".as_bytes()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tie_value_roundtrip() {
        let dir = tmp("tie_val");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tie_entity_ref() {
        let dir = tmp("tie_eref");
        let mut eng = Engine::create(&dir).unwrap();
        let parent = eng.entity();
        let child = eng.entity();
        eng.tie(child, "company", parent);
        assert_eq!(eng.get(child, "company"), Some(parent));
        eng.rebuild();
        let result = eng.pull_raw("company", parent);
        assert_eq!(result, vec![child]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tie_overwrite() {
        let dir = tmp("tie_ow");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "score", 100);
        eng.tie(e, "score", 200);
        assert_eq!(eng.get(e, "score"), Some(200));
        assert_eq!(eng.query_count(&[("score", 100)]), 0);
        assert_eq!(eng.query_count(&[("score", 200)]), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tie_value_zero() {
        let dir = tmp("tie_zero");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "level", 0);
        assert_eq!(eng.get(e, "level"), Some(0));
        assert_eq!(eng.query_count(&[("level", 0)]), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── untie ────

    #[test]
    fn untie_removes_value() {
        let dir = tmp("untie");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "X");

        eng.untie(e, "age");
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        assert_eq!(eng.get_text(e, "name"), Some(b"X".as_ref()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── delete ────

    #[test]
    fn delete_removes_all_ties() {
        let dir = tmp("del_ties");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "田中");

        eng.delete(e);
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.get_text(e, "name"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── content ────

    #[test]
    fn content_set_get() {
        let dir = tmp("content");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.content(e, "memo", b"hello");
        eng.content(e, "notes", "日本語".as_bytes());
        assert_eq!(eng.get_content(e, "memo"), Some(b"hello".as_ref()));
        assert_eq!(eng.get_content(e, "notes"), Some("日本語".as_bytes()));
        assert_eq!(eng.get_content(e, "none"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── himos_of / himo_names ────

    #[test]
    fn himos_of_entity() {
        let dir = tmp("himos_of");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "X");
        let h = eng.himos_of(e);
        assert!(h.contains(&"age"));
        assert!(h.contains(&"name"));
        assert_eq!(h.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn himo_names_all() {
        let dir = tmp("himo_names");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "x", 1);
        eng.tie_text(e, "y", "a");
        eng.tie(e, "z", e);
        let names = eng.himo_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"x".to_string()));
        assert!(names.contains(&"y".to_string()));
        assert!(names.contains(&"z".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── query ────

    #[test]
    fn query_single_condition() {
        let dir = tmp("q_single");
        let mut eng = Engine::create(&dir).unwrap();
        let e0 = eng.entity();
        eng.tie(e0, "age", 30);
        let e1 = eng.entity();
        eng.tie(e1, "age", 25);
        let e2 = eng.entity();
        eng.tie(e2, "age", 30);

        let result = eng.query(&[("age", 30)]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&e0));
        assert!(result.contains(&e2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn query_multi_condition() {
        let dir = tmp("q_multi");
        let mut eng = Engine::create(&dir).unwrap();

        let e0 = eng.entity();
        eng.tie(e0, "age", 30);
        eng.tie(e0, "dept", 1);

        let e1 = eng.entity();
        eng.tie(e1, "age", 25);
        eng.tie(e1, "dept", 1);

        let e2 = eng.entity();
        eng.tie(e2, "age", 30);
        eng.tie(e2, "dept", 2);

        assert_eq!(eng.query(&[("dept", 1), ("age", 30)]), vec![e0]);
        assert_eq!(eng.query(&[("dept", 1), ("age", 25)]), vec![e1]);
        assert_eq!(eng.query(&[("dept", 2), ("age", 30)]), vec![e2]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn query_empty_result() {
        let dir = tmp("q_empty");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert!(eng.query(&[("age", 99)]).is_empty());
        assert_eq!(eng.query_count(&[("age", 99)]), 0);
        assert!(eng.query(&[("nonexistent", 1)]).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn query_count_matches_len() {
        let dir = tmp("q_count");
        let mut eng = Engine::create(&dir).unwrap();
        for i in 0..10 {
            let e = eng.entity();
            eng.tie(e, "bucket", i % 3);
        }
        for b in 0..3 {
            let q = eng.query(&[("bucket", b)]);
            let c = eng.query_count(&[("bucket", b)]);
            assert_eq!(q.len(), c);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── LazyCylinder ────

    #[test]
    fn lazy_cylinder_pull_observe() {
        let dir = tmp("lazy_cyl");
        let mut eng = Engine::create(&dir).unwrap();

        let e0 = eng.entity();
        eng.tie(e0, "age", 30);
        eng.tie(e0, "dept", 1);

        let e1 = eng.entity();
        eng.tie(e1, "age", 25);
        eng.tie(e1, "dept", 1);

        assert_eq!(eng.query(&[("dept", 1), ("age", 30)]), vec![e0]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── range query ────

    #[test]
    fn pull_range() {
        let dir = tmp("range");
        let mut eng = Engine::create(&dir).unwrap();
        for age in 20..=40 {
            let e = eng.entity();
            eng.tie(e, "age", age);
        }
        eng.rebuild();
        let mut total = 0;
        for age in 25..=30 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, 6);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lazy_cylinder_pull_range() {
        let dir = tmp("lc_range");
        let mut eng = Engine::create(&dir).unwrap();

        for age in 20..=40 {
            let e = eng.entity();
            eng.tie(e, "age", age);
            eng.tie(e, "dept", 1);
        }

        eng.rebuild();
        let mut age_ents: Vec<u32> = Vec::new();
        for age in 25..=30 {
            age_ents.extend(eng.pull_raw("age", age));
        }
        age_ents.sort_unstable();
        let dept1 = eng.pull_raw("dept", 1);
        let mut count = 0;
        let (mut i, mut j) = (0, 0);
        while i < age_ents.len() && j < dept1.len() {
            match age_ents[i].cmp(&dept1[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => { count += 1; i += 1; j += 1; }
            }
        }
        assert_eq!(count, 6);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 永続化 ────

    #[test]
    fn persistence_full_roundtrip() {
        let dir = tmp("persist");

        {
            let mut eng = Engine::create(&dir).unwrap();
            let e0 = eng.entity();
            eng.tie(e0, "age", 25);
            eng.tie(e0, "dept", 1);

            let e1 = eng.entity();
            eng.tie(e1, "age", 30);
            eng.tie(e1, "dept", 1);
            eng.content(e1, "memo", b"hello");

            eng.flush().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.entity_count(), 2);
        assert_eq!(eng.get(0, "age"), Some(25));
        assert_eq!(eng.get(1, "age"), Some(30));
        assert_eq!(eng.get_content(1, "memo"), Some(b"hello".as_ref()));
        assert_eq!(eng.query_count(&[("dept", 1), ("age", 30)]), 1);

        let e2 = eng.entity();
        eng.tie(e2, "age", 35);
        eng.tie(e2, "dept", 1);
        assert_eq!(eng.query_count(&[("dept", 1), ("age", 35)]), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── vocab ────

    #[test]
    fn vocab_id_lookup() {
        let dir = tmp("vocab");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie_text(e, "city", "東京");
        eng.tie_text(e, "city2", "大阪");

        assert!(eng.vocab_id("東京").is_some());
        assert!(eng.vocab_id("大阪").is_some());
        assert!(eng.vocab_id("福岡").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 境界値 ────

    #[test]
    fn boundary_value_zero() {
        let dir = tmp("bnd_zero");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "x", 0);
        assert_eq!(eng.get(e, "x"), Some(0));
        assert_eq!(eng.query_count(&[("x", 0)]), 1);

        eng.untie(e, "x");
        assert_eq!(eng.get(e, "x"), None);
        eng.tie(e, "x", 0);
        assert_eq!(eng.get(e, "x"), Some(0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn boundary_value_large() {
        let dir = tmp("bnd_large");
        let mut eng = Engine::create(&dir).unwrap();

        let ts = 1_743_552_000u32;
        let e = eng.entity();
        eng.tie(e, "ts", ts);
        assert_eq!(eng.get(e, "ts"), Some(ts));
        eng.rebuild();
        let result = eng.pull_raw("ts", ts);
        assert_eq!(result, vec![e]);

        let big = u32::MAX - 2;
        let e2 = eng.entity();
        eng.tie(e2, "huge", big);
        assert_eq!(eng.get(e2, "huge"), Some(big));
        eng.rebuild();
        let result2 = eng.pull_raw("huge", big);
        assert_eq!(result2, vec![e2]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn boundary_consecutive_values() {
        let dir = tmp("bnd_consec");
        let mut eng = Engine::create(&dir).unwrap();
        for v in 0..5u32 {
            let e = eng.entity();
            eng.tie(e, "level", v);
        }
        for v in 0..5u32 {
            assert_eq!(eng.query_count(&[("level", v)]), 1);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn boundary_many_dims() {
        let dir = tmp("bnd_dims");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        for d in 0..20u32 {
            eng.tie(e, &format!("dim_{d}"), d * 10);
        }
        for d in 0..20u32 {
            assert_eq!(eng.get(e, &format!("dim_{d}")), Some(d * 10));
        }
        assert_eq!(eng.himos_of(e).len(), 20);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 大量削除 → query整合性 ────

    #[test]
    fn bulk_delete_query_consistency() {
        let dir = tmp("bulk_del");
        let mut eng = Engine::create(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "group", i % 5);
            eng.tie(e, "score", (i / 5) % 10);
        }

        let group0: Vec<u32> = eng.query(&[("group", 0)]);
        assert_eq!(group0.len(), 200);
        for &eid in &group0 {
            eng.delete(eid);
        }

        assert_eq!(eng.query_count(&[("group", 0)]), 0);
        for g in 1..5u32 {
            assert_eq!(eng.query_count(&[("group", g)]), 200);
        }
        assert_eq!(eng.entity_count(), 800);
        for s in 0..10u32 {
            assert_eq!(eng.query_count(&[("score", s)]), 80);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_all_then_reinsert() {
        let dir = tmp("del_all");
        let mut eng = Engine::create(&dir).unwrap();
        let n = 100u32;
        for _ in 0..n {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 100);

        let all: Vec<u32> = eng.entities();
        for eid in all {
            eng.delete(eid);
        }
        assert_eq!(eng.entity_count(), 0);
        assert_eq!(eng.query_count(&[("val", 42)]), 0);

        for _ in 0..50 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.entity_count(), 50);
        assert_eq!(eng.query_count(&[("val", 42)]), 50);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 永続化の堅牢性 ────

    #[test]
    fn persistence_after_delete() {
        let dir = tmp("persist_del");
        {
            let mut eng = Engine::create(&dir).unwrap();
            for i in 0..100u32 {
                let e = eng.entity();
                eng.tie(e, "val", i % 10);
            }
            let del_targets: Vec<u32> = eng.query(&[("val", 0)]);
            for &eid in &del_targets {
                eng.delete(eid);
            }
            eng.flush().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.query_count(&[("val", 0)]), 0);
        for v in 1..10u32 {
            assert_eq!(eng.query_count(&[("val", v)]), 10);
        }
        assert_eq!(eng.entity_count(), 90);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 多数 entity ────

    #[test]
    fn many_entities_1k() {
        let dir = tmp("many_1k");
        let mut eng = Engine::create(&dir).unwrap();
        let n = 1000u32;
        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "val", i % 10);
        }
        assert_eq!(eng.entity_count(), n as usize);
        for b in 0..10 {
            assert_eq!(eng.query_count(&[("val", b)]), 100);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 100万 entity スケールテスト ────

    const SCALE_N: u32 = 1_000_000;
    const SCALE_COMPANIES: u32 = 100;
    const SCALE_CITIES: u32 = 10;
    const SCALE_AGES: u32 = 50;
    const SCALE_DEPTS: u32 = 8;
    const SCALE_PER_CO: u32 = SCALE_N / SCALE_COMPANIES;

    fn setup_scale(dir: &str) -> Engine {
        let mut eng = Engine::create(dir).unwrap();

        for c in 0..SCALE_COMPANIES {
            for e in 0..SCALE_PER_CO {
                let eid = eng.entity();
                eng.tie(eid, "age", e % SCALE_AGES);
                eng.tie(eid, "dept", (e / SCALE_AGES) % SCALE_DEPTS);
                eng.tie(eid, "company", c);
                eng.tie_text(eid, "city", &format!("city_{}", c % SCALE_CITIES));
            }
        }
        eng
    }

    #[test]
    #[ignore]
    fn scale_insert_1m() {
        let dir = tmp("scale_insert");
        let eng = setup_scale(&dir);
        assert_eq!(eng.entity_count(), SCALE_N as usize);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_point_query() {
        let dir = tmp("scale_point");
        let mut eng = setup_scale(&dir);
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(eng.query_count(&[("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_multi_condition() {
        let dir = tmp("scale_multi");
        let mut eng = setup_scale(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let expected = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_three_conditions() {
        let dir = tmp("scale_3cond");
        let mut eng = setup_scale(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let per_co = SCALE_PER_CO / SCALE_AGES / SCALE_DEPTS;
        let expected = (SCALE_COMPANIES / SCALE_CITIES * per_co) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30), ("dept", 3)]), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_range_query() {
        let dir = tmp("scale_range");
        let mut eng = setup_scale(&dir);
        eng.rebuild();
        let per_age = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let mut total = 0;
        for age in 25..=34 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, per_age * 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_empty_result() {
        let dir = tmp("scale_empty");
        let mut eng = setup_scale(&dir);
        assert_eq!(eng.query_count(&[("age", 99)]), 0);
        assert!(eng.query(&[("age", 99)]).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_delete_reinsert() {
        let dir = tmp("scale_delins");
        let mut eng = setup_scale(&dir);
        let before = eng.query_count(&[("age", 30)]);

        let victims: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
        for eid in &victims {
            eng.delete(*eid);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before - 100);

        for _ in 0..100 {
            let e = eng.entity();
            eng.tie(e, "age", 30);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_update() {
        let dir = tmp("scale_upd");
        let mut eng = setup_scale(&dir);
        let before_30 = eng.query_count(&[("age", 30)]);
        assert_eq!(eng.query_count(&[("age", 99)]), 0);

        let targets: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
        for eid in &targets {
            eng.tie(*eid, "age", 99);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before_30 - 500);
        assert_eq!(eng.query_count(&[("age", 99)]), 500);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_persistence() {
        let dir = tmp("scale_persist");
        let city0_vid;
        let expected_age30 = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let expected_city_age = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        {
            let mut eng = setup_scale(&dir);
            city0_vid = eng.vocab_id("city_0").unwrap();
            eng.flush().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.entity_count(), SCALE_N as usize);
        assert_eq!(eng.query_count(&[("age", 30)]), expected_age30);
        assert_eq!(eng.query_count(&[("city", city0_vid), ("age", 30)]), expected_city_age);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn scale_group_by_equivalent() {
        let dir = tmp("scale_grp");
        let mut eng = setup_scale(&dir);
        let mut total = 0usize;
        for c in 0..SCALE_CITIES {
            let vid = eng.vocab_id(&format!("city_{c}")).unwrap();
            total += eng.query_count(&[("city", vid), ("age", 30)]);
        }
        let expected_total = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(total, expected_total);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── トランザクション ────

    #[test]
    fn commit_persists() {
        let dir = tmp("tx_commit");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.commit();
        eng.flush().unwrap();
        drop(eng);

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rollback_reverts() {
        let dir = tmp("tx_rollback");
        let mut eng = Engine::create(&dir).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.commit();

        eng.tie(e, "age", 99);
        assert_eq!(eng.get(e, "age"), Some(99));
        eng.rollback();
        assert_eq!(eng.get(e, "age"), Some(30));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rollback_insert() {
        let dir = tmp("tx_rb_ins");
        let mut eng = Engine::create(&dir).unwrap();
        eng.commit();

        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.query_count(&[("age", 30)]), 1);
        eng.rollback();
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_recovery_rollback() {
        let dir = tmp("tx_crash");
        {
            let mut eng = Engine::create(&dir).unwrap();
            let e = eng.entity();
            eng.tie(e, "age", 30);
            eng.commit();
            eng.flush().unwrap();

            eng.tie(e, "age", 99);
            eng.undo.flush().unwrap();
        }

        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.get(0, "age"), Some(30));
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── prefix sum O(1) ────

    #[test]
    fn prefix_sum_point_query() {
        let dir = tmp("ps_point");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 100); // max_values=100 → prefix sum
        eng.define_himo("dept", HimoType::Value, 20);

        for i in 0..1000u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 50);
            eng.tie(e, "dept", i % 8);
        }
        // age=30 → 20件
        assert_eq!(eng.query_count(&[("age", 30)]), 20);
        // dept=3 → 125件
        assert_eq!(eng.query_count(&[("dept", 3)]), 125);
        // age=30 AND dept=2 → 5件 (i=130,330,530,730,930)
        assert_eq!(eng.query(&[("age", 30), ("dept", 2)]).len(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_value_zero() {
        let dir = tmp("ps_zero");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("level", HimoType::Value, 10);
        let e = eng.entity();
        eng.tie(e, "level", 0);
        assert_eq!(eng.get(e, "level"), Some(0));
        assert_eq!(eng.query_count(&[("level", 0)]), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_mixed_with_bsearch() {
        // age は prefix sum、name は二分探索（max_values 未指定）
        let dir = tmp("ps_mixed");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 100);

        for i in 0..100u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 10);
            eng.tie_text(e, "city", if i < 50 { "東京" } else { "大阪" });
        }
        let tokyo = eng.vocab_id("東京").unwrap();
        // age=5 → 10件, city=東京 → 50件, AND → 5件
        assert_eq!(eng.query_count(&[("age", 5)]), 10);
        assert_eq!(eng.query_count(&[("city", tokyo)]), 50);
        assert_eq!(eng.query_count(&[("age", 5), ("city", tokyo)]), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_persistence() {
        let dir = tmp("ps_persist");
        {
            let mut eng = Engine::create(&dir).unwrap();
            eng.define_himo("score", HimoType::Value, 200);
            for i in 0..100u32 {
                let e = eng.entity();
                eng.tie(e, "score", i % 20);
            }
            assert_eq!(eng.query_count(&[("score", 5)]), 5);
            eng.flush().unwrap();
        }
        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.query_count(&[("score", 5)]), 5);
        assert_eq!(eng.entity_count(), 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_untie() {
        let dir = tmp("ps_untie");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 100);
        let e = eng.entity();
        eng.tie(e, "age", 30);
        assert_eq!(eng.query_count(&[("age", 30)]), 1);
        eng.untie(e, "age");
        assert_eq!(eng.get(e, "age"), None);
        assert_eq!(eng.query_count(&[("age", 30)]), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_overwrite() {
        let dir = tmp("ps_ow");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("score", HimoType::Value, 1000);
        let e = eng.entity();
        eng.tie(e, "score", 100);
        eng.tie(e, "score", 200);
        assert_eq!(eng.get(e, "score"), Some(200));
        assert_eq!(eng.query_count(&[("score", 100)]), 0);
        assert_eq!(eng.query_count(&[("score", 200)]), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_delete() {
        let dir = tmp("ps_del");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("age", HimoType::Value, 100);
        eng.define_himo("dept", HimoType::Value, 20);
        for i in 0..100u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 10);
            eng.tie(e, "dept", i % 5);
        }
        let victims: Vec<u32> = eng.query(&[("age", 0)]);
        assert_eq!(victims.len(), 10);
        for &eid in &victims { eng.delete(eid); }
        assert_eq!(eng.query_count(&[("age", 0)]), 0);
        for a in 1..10u32 {
            assert_eq!(eng.query_count(&[("age", a)]), 10);
        }
        assert_eq!(eng.entity_count(), 90);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_rollback() {
        let dir = tmp("ps_rollback");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("val", HimoType::Value, 50);
        let e = eng.entity();
        eng.tie(e, "val", 10);
        eng.commit();

        eng.tie(e, "val", 40);
        assert_eq!(eng.get(e, "val"), Some(40));
        eng.rollback();
        assert_eq!(eng.get(e, "val"), Some(10));
        assert_eq!(eng.query_count(&[("val", 10)]), 1);
        assert_eq!(eng.query_count(&[("val", 40)]), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_boundary_max() {
        // max_values ちょうどの値を使う
        let dir = tmp("ps_bnd_max");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("x", HimoType::Value, 10);
        let e = eng.entity();
        eng.tie(e, "x", 10); // max_values と同値
        assert_eq!(eng.get(e, "x"), Some(10));
        assert_eq!(eng.query_count(&[("x", 10)]), 1);
        // max_values を超える値はアクセスできるが binary search にフォールバック
        // ただし超えたらCylinderのprefix領域外なのでbsearchになる
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_sum_bulk_delete_reinsert() {
        let dir = tmp("ps_bulk");
        let mut eng = Engine::create(&dir).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        for _ in 0..500u32 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.query_count(&[("val", 42)]), 500);

        let all: Vec<u32> = eng.entities();
        for eid in all { eng.delete(eid); }
        assert_eq!(eng.entity_count(), 0);
        assert_eq!(eng.query_count(&[("val", 42)]), 0);

        for _ in 0..200 {
            let e = eng.entity();
            eng.tie(e, "val", 42);
        }
        assert_eq!(eng.entity_count(), 200);
        assert_eq!(eng.query_count(&[("val", 42)]), 200);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── prefix sum スケールテスト（100万 entity）────

    fn setup_scale_prefix(dir: &str) -> Engine {
        let mut eng = Engine::create(dir).unwrap();
        eng.define_himo("age", HimoType::Value, SCALE_AGES);
        eng.define_himo("dept", HimoType::Value, SCALE_DEPTS);
        eng.define_himo("company", HimoType::Value, SCALE_COMPANIES);
        // city は Symbol → vocab 経由、max_values 未指定（binary search）

        for c in 0..SCALE_COMPANIES {
            for e in 0..SCALE_PER_CO {
                let eid = eng.entity();
                eng.tie(eid, "age", e % SCALE_AGES);
                eng.tie(eid, "dept", (e / SCALE_AGES) % SCALE_DEPTS);
                eng.tie(eid, "company", c);
                eng.tie_text(eid, "city", &format!("city_{}", c % SCALE_CITIES));
            }
        }
        eng
    }

    #[test]
    #[ignore]
    fn ps_scale_insert_1m() {
        let dir = tmp("ps_scale_ins");
        let eng = setup_scale_prefix(&dir);
        assert_eq!(eng.entity_count(), SCALE_N as usize);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_point_query() {
        let dir = tmp("ps_scale_point");
        let mut eng = setup_scale_prefix(&dir);
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(eng.query_count(&[("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_multi_condition() {
        let dir = tmp("ps_scale_multi");
        let mut eng = setup_scale_prefix(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let expected = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30)]), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_three_conditions() {
        let dir = tmp("ps_scale_3cond");
        let mut eng = setup_scale_prefix(&dir);
        let city0 = eng.vocab_id("city_0").unwrap();
        let per_co = SCALE_PER_CO / SCALE_AGES / SCALE_DEPTS;
        let expected = (SCALE_COMPANIES / SCALE_CITIES * per_co) as usize;
        assert_eq!(eng.query_count(&[("city", city0), ("age", 30), ("dept", 3)]), expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_range_query() {
        let dir = tmp("ps_scale_range");
        let mut eng = setup_scale_prefix(&dir);
        eng.rebuild();
        let per_age = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let mut total = 0;
        for age in 25..=34 {
            total += eng.pull_raw("age", age).len();
        }
        assert_eq!(total, per_age * 10);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_delete_reinsert() {
        let dir = tmp("ps_scale_delins");
        let mut eng = setup_scale_prefix(&dir);
        let before = eng.query_count(&[("age", 30)]);
        let victims: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(100).collect();
        for eid in &victims { eng.delete(*eid); }
        assert_eq!(eng.query_count(&[("age", 30)]), before - 100);
        for _ in 0..100 {
            let e = eng.entity();
            eng.tie(e, "age", 30);
        }
        assert_eq!(eng.query_count(&[("age", 30)]), before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_update() {
        let dir = tmp("ps_scale_upd");
        let mut eng = setup_scale_prefix(&dir);
        let before_30 = eng.query_count(&[("age", 30)]);
        let targets: Vec<u32> = eng.query(&[("age", 30)]).into_iter().take(500).collect();
        for eid in &targets { eng.tie(*eid, "age", 49); }
        assert_eq!(eng.query_count(&[("age", 30)]), before_30 - 500);
        assert_eq!(eng.query_count(&[("age", 49)]),
            (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize + 500);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_persistence() {
        let dir = tmp("ps_scale_persist");
        let city0_vid;
        let expected_age30 = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        let expected_city_age = (SCALE_COMPANIES / SCALE_CITIES * SCALE_PER_CO / SCALE_AGES) as usize;
        {
            let mut eng = setup_scale_prefix(&dir);
            city0_vid = eng.vocab_id("city_0").unwrap();
            eng.flush().unwrap();
        }
        let mut eng = Engine::open(&dir).unwrap();
        assert_eq!(eng.entity_count(), SCALE_N as usize);
        assert_eq!(eng.query_count(&[("age", 30)]), expected_age30);
        assert_eq!(eng.query_count(&[("city", city0_vid), ("age", 30)]), expected_city_age);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn ps_scale_group_by() {
        let dir = tmp("ps_scale_grp");
        let mut eng = setup_scale_prefix(&dir);
        let mut total = 0usize;
        for c in 0..SCALE_CITIES {
            let vid = eng.vocab_id(&format!("city_{c}")).unwrap();
            total += eng.query_count(&[("city", vid), ("age", 30)]);
        }
        let expected = (SCALE_PER_CO / SCALE_AGES * SCALE_COMPANIES) as usize;
        assert_eq!(total, expected);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ──── 1億 entity ────

    #[test]
    #[ignore]
    fn scale_100m_insert_and_query() {
        let dir = tmp("scale_100m");
        let n = 100_000_000u32;
        let ages = 100u32;
        let depts = 20u32;
        let groups = 1000u32;

        let mut eng = Engine::create_with_capacity(&dir, n + 1024).unwrap();
        eng.define_himo("age", HimoType::Value, ages);
        eng.define_himo("dept", HimoType::Value, depts);
        eng.define_himo("group", HimoType::Value, groups);

        for i in 0..n {
            let e = eng.entity();
            eng.tie(e, "age", i % ages);
            eng.tie(e, "dept", i % depts);
            eng.tie(e, "group", i % groups);
            if i % 1_000_000 == 999_999 { eng.commit(); } // undo バッファ溢れ防止
        }
        assert_eq!(eng.entity_count(), n as usize);

        // age=50 → n/100 = 1,000,000
        assert_eq!(eng.query_count(&[("age", 50)]), (n / ages) as usize);
        // dept=10 → n/20 = 5,000,000
        assert_eq!(eng.query_count(&[("dept", 10)]), (n / depts) as usize);
        // age=50 AND dept=10 → n/lcm(100,20) = n/100 = 1,000,000
        // 実際: i%100==50 AND i%20==10 → i%100==50 (50%20==10✓) → n/100
        assert_eq!(eng.query_count(&[("age", 50), ("dept", 10)]), (n / ages) as usize);
        // age=50 AND group=500 → i%100==50 AND i%1000==500
        // i=500,1500,...,99999500 → n/1000 ではなく lcm(100,1000)=1000 周期で i%100==50 AND i%1000==500 → 500%100==0≠50...
        // i%1000==500 の中で i%100==50 → 500%100=0≠50。i%1000 が x で x%100==50 → x=50,150,250,...,950 → 10個/1000周期
        // n/1000 * 10 = 1,000,000
        assert_eq!(eng.query_count(&[("age", 50), ("group", 500)]), 0); // 500%100=0≠50
        assert_eq!(eng.query_count(&[("age", 50), ("group", 50)]), (n / groups) as usize); // 50%100==50✓

        // 3条件: age=30 AND dept=10 AND group=30
        // i%100==30 AND i%20==10 → 30%20=10✓ → i%100==30
        // AND i%1000==30 → 30%100=30✓ → i%1000==30 → n/1000 = 100,000
        assert_eq!(eng.query_count(&[("age", 30), ("dept", 10), ("group", 30)]), (n / groups) as usize);

        // get
        assert_eq!(eng.get(50, "age"), Some(50));
        assert_eq!(eng.get(50, "dept"), Some(50 % depts));

        // delete 1000件
        let victims: Vec<u32> = eng.query(&[("age", 99)]).into_iter().take(1000).collect();
        for &eid in &victims { eng.delete(eid); }
        assert_eq!(eng.query_count(&[("age", 99)]), (n / ages) as usize - 1000);
        assert_eq!(eng.entity_count(), (n - 1000) as usize);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
