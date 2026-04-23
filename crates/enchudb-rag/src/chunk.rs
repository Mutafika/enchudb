//! Chunk とメタデータの型。

/// メタデータ値。
#[derive(Clone, Debug)]
pub enum MetaValue {
    /// 整数値（そのまま tie される）。例: tenant_id、price、epoch 日数。
    Value(u32),
    /// 文字列（Vocabulary 経由で ID 化されて tie される）。例: lang="ja"、category="news"。
    Symbol(String),
    /// entity 参照（他 chunk への紐付け）。
    Ref(u64),
}

/// メタデータ集合。挿入順を保つ。
#[derive(Clone, Debug, Default)]
pub struct Meta {
    pub(crate) entries: Vec<(String, MetaValue)>,
}

impl Meta {
    pub fn new() -> Self { Self { entries: Vec::new() } }

    pub fn value(mut self, key: impl Into<String>, v: u32) -> Self {
        self.entries.push((key.into(), MetaValue::Value(v)));
        self
    }

    pub fn symbol(mut self, key: impl Into<String>, v: impl Into<String>) -> Self {
        self.entries.push((key.into(), MetaValue::Symbol(v.into())));
        self
    }

    pub fn reference(mut self, key: impl Into<String>, v: u64) -> Self {
        self.entries.push((key.into(), MetaValue::Ref(v)));
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = &(String, MetaValue)> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
}

/// 挿入する chunk。
#[derive(Clone, Debug)]
pub struct Chunk {
    pub text: String,
    pub vector: Vec<f32>,
    pub meta: Meta,
}

impl Chunk {
    pub fn new(text: impl Into<String>, vector: Vec<f32>) -> Self {
        Self { text: text.into(), vector, meta: Meta::new() }
    }

    pub fn with_meta(mut self, meta: Meta) -> Self {
        self.meta = meta;
        self
    }
}
