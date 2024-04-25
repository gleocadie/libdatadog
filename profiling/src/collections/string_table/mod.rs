// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use crate::collections::identifiable::{Id, StringId};
use crate::iter::{IntoLendingIterator, LendingIterator};
use datadog_alloc::{Allocator, ChainAllocator, VirtualAllocator};
use std::alloc::Layout;

type Hasher = core::hash::BuildHasherDefault<rustc_hash::FxHasher>;
type HashSet<K> = indexmap::IndexSet<K, Hasher>;

/// Holds unique strings and provides [StringId]s that correspond to the order
/// that the strings were inserted.
pub struct StringTable {
    /// The bytes of each string stored in `strings` are allocated here.
    bytes: ChainAllocator<VirtualAllocator>,

    /// The ordered hash set of unique strings. The order becomes the StringId.
    /// The static lifetime is a lie, it is tied to the `bytes`, which is only
    /// moved if the string table is moved e.g.
    /// [StringTable::into_lending_iterator].
    /// References to the underlying strings should generally not be handed,
    /// but if they are, they should be bound to the string table's lifetime
    /// or the lending iterator's lifetime.
    strings: HashSet<&'static str>,
}

impl Default for StringTable {
    fn default() -> Self {
        Self::new()
    }
}

impl StringTable {
    /// Creates a new string table, which initially holds the empty string and
    /// no others.
    pub fn new() -> Self {
        // Christophe and Grégory think this is a fine size for 32-bit .NET.
        const SIZE_HINT: usize = 4 * 1024 * 1024;
        let bytes = ChainAllocator::new_in(SIZE_HINT, VirtualAllocator {});

        let mut strings = HashSet::with_hasher(Hasher::default());
        // It various by implementation, but frequently I've noticed that the
        // capacity after the first insertion is quite small, as in 3. This is
        // a bit too small and there are frequent reallocations. For one sample
        // with endpoint + code hotspots, we'd have at least these strings:
        // - ""
        // - At least one sample type
        // - At least one sample unit--already at 3 without any samples.
        // - "local root span id"
        // - "span id"
        // - "trace endpoint"
        // - A file and/or function name per frame.
        // So with a capacity like 3, we end up reallocating a bunch on or
        // before the very first sample. The number here is not fine-tuned,
        // just skipping some obviously bad, tiny sizes.
        strings.reserve(32);

        // Always hold the empty string as item 0. Do not insert it via intern
        // because that will try to allocate zero-bytes from the storage,
        // which is sketchy.
        strings.insert("");

        Self { bytes, strings }
    }

    /// Returns the number of strings currently held in the string table.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.strings.len()
    }

    /// Adds the string to the string table if it isn't present already, and
    /// returns a [StringId] that corresponds to the order that this string
    /// was originally inserted.
    ///
    /// # Panics
    /// This panics if the allocator fails to allocate. This could happen for
    /// a few reasons:
    ///  - It failed to acquire a chunk.
    ///  - The string is too large (larger than a chunk size, for example)
    pub fn intern(&mut self, str: &str) -> StringId {
        let set = &mut self.strings;
        match set.get_index_of(str) {
            Some(offset) => StringId::from_offset(offset),
            None => {
                // No match. Get the current size of the table, which
                // corresponds to the StringId it will have when inserted.
                let string_id = StringId::from_offset(set.len());

                // Allocate the string with alignment of 1 so that bytes are
                // not wasted between strings.
                let uninit_ptr = {
                    // SAFETY: this layout matches an existing string, it must
                    // be a valid layout since it already exists.
                    let layout =
                        unsafe { Layout::from_size_align(str.len(), 1).unwrap_unchecked() };
                    self.bytes.allocate(layout).unwrap()
                };

                // Copy the bytes of the string into the allocated memory.
                // SAFETY: this is guaranteed to not be overlapping because an
                // allocator must not return aliasing bytes in its allocations.
                unsafe {
                    let src = str.as_ptr();
                    let dst = uninit_ptr.as_ptr() as *mut u8;
                    let count = str.len();
                    core::ptr::copy_nonoverlapping(src, dst, count)
                };

                // Convert the bytes into a str, and insert into the set.

                // SAFETY: The bytes were properly initialized, and they cannot
                // be misaligned because they have an alignment of 1, so it is
                // safe to create a slice of the given data and length. The
                // static lifetime is _wrong_, and we need to be careful. Any
                // references returned to a caller must be tied to the lifetime
                // of the string table, or to the iterator if iterating.
                let slice: &'static [u8] = unsafe {
                    core::slice::from_raw_parts(uninit_ptr.as_ptr() as *const u8, str.len())
                };

                // SAFETY: Since the bytes were copied from a valid str without
                // slicing, the bytes must also be utf-8.
                let new_str = unsafe { core::str::from_utf8_unchecked(slice) };

                self.strings.insert(new_str);
                string_id
            }
        }
    }
}

/// A [LendingIterator] for a [StringTable]. Make one by calling
/// [StringTable::into_lending_iter].
pub struct StringTableIter {
    /// This is actually used, the compiler doesn't know that the static
    /// references in `iter` actually point in here.
    #[allow(unused)]
    bytes: ChainAllocator<VirtualAllocator>,

    /// The strings of the string table, in order of insertion.
    /// The static lifetimes are a lie, they are tied to the `bytes`. When
    /// handing out references, bind the lifetime to the iterator's lifetime,
    /// which is a [LendingIterator] is needed.
    iter: <HashSet<&'static str> as IntoIterator>::IntoIter,
}

impl StringTableIter {
    fn new(string_table: StringTable) -> StringTableIter {
        StringTableIter {
            bytes: string_table.bytes,
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

#[allow(unused)]
pub mod wordpress_test_data;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basics() {
        let mut table = StringTable::new();
        // The empty string should already be present.
        assert_eq!(1, table.len());
        assert_eq!(StringId::ZERO, table.intern(""));

        // Intern a string literal to ensure ?Sized works.
        let string = table.intern("datadog");
        assert_eq!(StringId::from_offset(1), string);
        assert_eq!(2, table.len());
    }

    #[track_caller]
    fn test_from_src(src: &[&str]) {
        // Insert all the strings.
        let mut table = StringTable::new();
        let n_strings = src.len();
        for string in src {
            table.intern(string);
        }
        assert_eq!(n_strings, table.len());

        // Re-inserting doesn't change the size.
        for string in src {
            table.intern(string);
        }
        assert_eq!(n_strings, table.len());

        // Check that they are ordered correctly when iterating.
        let mut actual_iter = table.into_lending_iter();
        let mut expected_iter = src.iter();
        while let (Some(expected), Some(actual)) = (expected_iter.next(), actual_iter.next()) {
            assert_eq!(*expected, actual);
        }

        // The iterators should be exhausted at this point.
        assert_eq!(None, expected_iter.next());
        assert_eq!(0, actual_iter.count());
    }

    #[test]
    fn test_small_set_of_strings() {
        let cases: &[_] = &[
            "",
            "local root span id",
            "span id",
            "trace endpoint",
            "samples",
            "count",
            "wall-time",
            "nanoseconds",
            "cpu-time",
            "<?php",
            "/srv/demo/public/index.php",
            "pid",
            "/var/www/public/index.php",
            "main",
            "thread id",
            "A\\Very\\Long\\Php\\Namespace\\Class::method",
            "/",
        ];
        test_from_src(cases);
    }

    /// Test inserting strings from a real WordPress profile, although it's
    /// unknown at this point which profiler version generated it. It's a small
    /// sample. Here we're checking that we don't panic or otherwise fail, and
    /// that the total number of strings and the bytes of those strings match
    /// the profile.
    #[test]
    fn test_wordpress() {
        test_from_src(&wordpress_test_data::WORDPRESS_STRINGS);
    }
}
