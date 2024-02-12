extern crate build_common;

use build_common::generate_header;
use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let header_name = "common.h"; // This can be parameterized as needed
    let output_base_dir = env::var("DESTDIR").ok(); // Use `ok()` to convert Result to Option

    generate_header(crate_dir, header_name, output_base_dir.as_deref());
}
