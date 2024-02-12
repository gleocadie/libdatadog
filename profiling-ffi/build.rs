use cbindgen;
use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let crate_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let cargo_target_dir = env::var("DESTDIR").unwrap_or_else(|_| "target".to_string());
    let header_name = "profiling.h".to_string();
    // Determine if `cargo_target_dir` is absolute or relative
    let cargo_target_path = Path::new(&cargo_target_dir);
    let output_path = if cargo_target_path.is_absolute() {
        // If absolute, use it directly
        cargo_target_path.join("include/datadog/").join(header_name)
    } else {
        // If relative, consider it with `../` prefix (as it probably contains "target")
        Path::new("..").join(cargo_target_path).join("include/datadog/").join(header_name)
    };

    // Ensure the output directory exists
    if let Some(parent) = output_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).expect("Failed to create output directory");
        }
    }

    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(cbindgen::Config::from_root_or_default(&crate_dir))
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(output_path);
}
