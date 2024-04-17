// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use crate::collections::identifiable::{Id, StringId};
use crate::iter::{IntoLendingIterator, LendingIterator};
use datadog_alloc::{Allocator, ChainAllocator, VirtualAllocator};
use std::alloc::Layout;
use std::borrow::Borrow;

type Hasher = core::hash::BuildHasherDefault<rustc_hash::FxHasher>;
type HashSet<K> = indexmap::IndexSet<K, Hasher>;

pub struct StringTable {
    string_storage: ChainAllocator<VirtualAllocator>,

    // The static lifetime is a lie, it is tied to the string_storage.
    // References to the underlying strings should not be handed out.
    strings: HashSet<&'static str>,
}

impl Default for StringTable {
    fn default() -> Self {
        Self::new()
    }
}

impl StringTable {
    pub fn new() -> Self {
        // Christophe and Grégory think this is a fine size for 32-bit .NET.
        const SIZE_HINT: usize = 4 * 1024 * 1024;
        Self {
            string_storage: ChainAllocator::new_in(SIZE_HINT, VirtualAllocator {}),
            strings: HashSet::with_hasher(Hasher::default()),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        // always holds the empty string
        self.strings.len() + 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        // always holds the empty string
        false
    }

    pub fn intern(&mut self, s: impl Borrow<str>) -> StringId {
        let str = s.borrow();
        if str.is_empty() {
            return StringId::ZERO;
        }

        let set = &mut self.strings;
        match set.get_index_of(str) {
            Some(offset) => StringId::from_offset(offset + 1),
            None => {
                // It's +1 because the empty str is not held, but the string
                // table acts as if it is.
                let len = set.len() + 1;
                let string_id = StringId::from_offset(len);

                // SAFETY: todo
                let uninit_ptr = unsafe {
                    self.string_storage
                        .allocate(Layout::from_size_align(str.len(), 1).unwrap_unchecked())
                }
                .unwrap();
                // SAFETY: todo
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        str.as_ptr(),
                        uninit_ptr.as_ptr() as *mut u8,
                        str.len(),
                    )
                };
                // SAFETY: todo
                self.strings.insert(unsafe {
                    core::str::from_utf8_unchecked(core::slice::from_raw_parts(
                        uninit_ptr.as_ptr() as *const u8,
                        str.len(),
                    ))
                });
                string_id
            }
        }
    }
}

pub struct StringTableIter {
    // This is actually used, the compiler doesn't know that the 'static
    // references actually point in here.
    #[allow(unused)]
    string_storage: ChainAllocator<VirtualAllocator>,

    // The static lifetime is a lie, it is tied to the string_storage.
    // References to the underlying strings should not be handed out.
    iter: <HashSet<&'static str> as IntoIterator>::IntoIter,
}

impl StringTableIter {
    fn new(string_table: StringTable) -> StringTableIter {
        StringTableIter {
            string_storage: string_table.string_storage,
            iter: string_table.strings.into_iter(),
        }
    }
}

impl LendingIterator for StringTableIter {
    type Item<'a> = &'a str where Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next()
    }

    fn count(self) -> usize {
        self.iter.count()
    }
}

impl IntoLendingIterator for StringTable {
    type Iter = StringTableIter;

    fn into_lending_iter(self) -> Self::Iter {
        StringTableIter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basics() {
        let mut table = StringTable::new();
        assert_eq!(StringId::ZERO, table.intern(""));

        let first_string = table.intern("datadog");
        assert_eq!(StringId::from_offset(1), first_string);
    }
}
