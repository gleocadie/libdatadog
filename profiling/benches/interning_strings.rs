// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use criterion::*;
use datadog_profiling::collections::string_table::StringTable;
use datadog_profiling::pprof;
use lz4_flex::frame::FrameDecoder;
use std::fs::File;
use std::io::{copy, Cursor};

pub fn small_wordpress_profile(c: &mut Criterion) {
    use prost::Message;

    let compressed_size = 101824_u64;
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/wordpress.pprof.lz4");
    let mut decoder = FrameDecoder::new(File::open(path).unwrap());
    let mut bytes = Vec::with_capacity(compressed_size as usize);
    copy(&mut decoder, &mut bytes).unwrap();

    let pprof = pprof::Profile::decode(&mut Cursor::new(&bytes)).unwrap();

    c.bench_function("benching string interning on wordpress profile", |b| {
        b.iter(|| {
            let mut table = StringTable::new();
            let n_strings = pprof.string_table.len();
            for string in &pprof.string_table {
                black_box(table.intern(string));
            }
            assert_eq!(n_strings, table.len());

            // re-insert, should nothing should be inserted.
            for string in &pprof.string_table {
                black_box(table.intern(string));
            }
            assert_eq!(n_strings, table.len())
        })
    });
}

criterion_group!(benches, small_wordpress_profile);
