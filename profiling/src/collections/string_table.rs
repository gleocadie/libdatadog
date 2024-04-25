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

    #[test]
    fn test_small_sample_equivalent_of_strings() {
        let cases: &[_] = &[
            (StringId::ZERO, ""),
            (StringId::from_offset(1), "local root span id"),
            (StringId::from_offset(2), "span id"),
            (StringId::from_offset(3), "trace endpoint"),
            (StringId::from_offset(4), "samples"),
            (StringId::from_offset(5), "count"),
            (StringId::from_offset(6), "wall-time"),
            (StringId::from_offset(7), "nanoseconds"),
            (StringId::from_offset(8), "cpu-time"),
            (StringId::from_offset(9), "<?php"),
            (StringId::from_offset(10), "/srv/demo/public/index.php"),
            (StringId::from_offset(11), "pid"),
            (StringId::from_offset(12), "/var/www/public/index.php"),
            (StringId::from_offset(13), "main"),
            (StringId::from_offset(14), "thread id"),
            (
                StringId::from_offset(15),
                "A\\Very\\Long\\Php\\Namespace\\Class::method",
            ),
            (StringId::from_offset(16), "/"),
        ];

        let mut table = StringTable::new();

        // Insert the cases, checking the string ids in turn.
        for (offset, str) in cases.iter() {
            let actual_offset = table.intern(str);
            assert_eq!(*offset, actual_offset);
        }
        assert_eq!(cases.len(), table.len());

        // Insert again to ensure they aren't re-added.
        for (string_id, str) in cases.iter() {
            let actual_string_id = table.intern(str);
            assert_eq!(*string_id, actual_string_id);
        }
        assert_eq!(cases.len(), table.len());

        // Check that they are ordered correctly when iterating.
        let mut table_iter = table.into_lending_iter();
        let mut case_iter = cases.iter();
        while let (Some(item), Some((_, case))) = (table_iter.next(), case_iter.next()) {
            assert_eq!(*case, item);
        }

        // The iterator should be exhausted at this point.
        assert_eq!(0, table_iter.count());
    }

    use crate::pprof;
    use lz4_flex::frame::FrameDecoder;
    use prost::Message;
    use std::fs::File;
    use std::io::{copy, Cursor};

    /// Test inserting strings from a real WordPress profile, although it's
    /// unknown at this point which profiler version generated it. It's a small
    /// sample. Here we're checking that we don't panic or otherwise fail, and
    /// that the total number of strings and the number of bytes of those
    /// strings match the profile.
    #[test]
    #[cfg_attr(miri, ignore)] // This test is too slow for miri
    fn test_wordpress() {
        // Load up the compressed profile.
        let compressed_size = 101824_u64;
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wordpress.pprof.lz4");
        let mut decoder = FrameDecoder::new(File::open(path).unwrap());
        let mut bytes = Vec::with_capacity(compressed_size as usize);
        copy(&mut decoder, &mut bytes).unwrap();

        let pprof = pprof::Profile::decode(&mut Cursor::new(&bytes)).unwrap();

        // Insert all the strings, calculating the correct number of strings
        // and string bytes.
        let mut table = StringTable::new();
        let n_strings = pprof.string_table.len();
        let mut expected_bytes = 0;
        for string in &pprof.string_table {
            expected_bytes += string.len();
            table.intern(string);
        }
        assert_eq!(n_strings, table.len());

        let mut string_iter = table.into_lending_iter();
        let mut actual_bytes = 0;
        while let Some(string) = string_iter.next() {
            actual_bytes += string.len();
        }
        assert_eq!(expected_bytes, actual_bytes);
    }
}
