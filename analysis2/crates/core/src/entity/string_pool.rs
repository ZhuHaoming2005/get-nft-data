//! Interned string pool backed by an ahash map.

use ahash::AHashMap;

use super::ids::StringId;

/// Global intern table for names, URIs, and other repeated strings.
#[derive(Clone, Debug, Default)]
pub struct StringPool {
    strings: Vec<String>,
    ids: AHashMap<String, StringId>,
}

impl StringPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.strings.reserve(additional);
        self.ids.reserve(additional);
    }

    pub fn get(&self, id: StringId) -> &str {
        &self.strings[id as usize]
    }

    pub fn lookup(&self, s: &str) -> Option<StringId> {
        self.ids.get(s).copied()
    }

    /// Intern `s`, returning the existing id on duplicate.
    pub fn intern(&mut self, s: &str) -> StringId {
        if let Some(&id) = self.ids.get(s) {
            return id;
        }
        let id = StringId::try_from(self.strings.len()).expect("too many interned strings");
        self.strings.push(s.to_owned());
        self.ids.insert(s.to_owned(), id);
        id
    }

    /// Empty (including whitespace-only) → `None`; otherwise intern the raw slice.
    pub fn intern_nonempty(&mut self, s: &str) -> Option<StringId> {
        if s.trim().is_empty() {
            None
        } else {
            Some(self.intern(s))
        }
    }

    /// Empty / blank after trim → `None`; otherwise intern the trimmed value.
    pub fn intern_nonblank(&mut self, s: &str) -> Option<StringId> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(self.intern(trimmed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StringPool;

    #[test]
    fn string_pool_intern_deduplicates() {
        let mut pool = StringPool::default();
        let a = pool.intern("alpha");
        let b = pool.intern("alpha");
        let c = pool.intern("beta");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(pool.get(a), "alpha");
        assert_eq!(pool.get(c), "beta");
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn string_pool_empty_helpers_return_none() {
        let mut pool = StringPool::default();
        assert_eq!(pool.intern_nonempty(""), None);
        assert_eq!(pool.intern_nonempty("   \t"), None);
        assert_eq!(pool.intern_nonblank(""), None);
        assert_eq!(pool.intern_nonblank(" \t "), None);

        let gamma = pool.intern_nonempty("gamma").expect("non-empty");
        assert_eq!(pool.get(gamma), "gamma");
        // nonempty does not trim: leading spaces are kept as part of the value
        let spaced = pool.intern_nonempty("  keep ").expect("spaces kept");
        assert_eq!(pool.get(spaced), "  keep ");

        let delta = pool.intern_nonblank("  delta  ").expect("non-blank");
        assert_eq!(pool.get(delta), "delta");
        assert_eq!(pool.intern_nonblank("delta"), Some(delta));
        assert_eq!(pool.len(), 3);
    }
}
