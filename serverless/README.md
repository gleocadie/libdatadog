# Agentless for Serverless

## To manually compile a native node addon:
Run in the `serverless` directory:
- `yarn build`
- Note: this runs `napi build --platform --release --features build_for_node`, where the feature flag `build_for_node` makes the build include the node specific `send_trace_node` (as opposed the the regular `send_trace` function called using FFI in other languages)

## To manually compile a dynamic library:
Run in the `serverless` directory:
- `cargo build --release --target your-target`
- The compiled dynamic library will be located in `target/release` in the libdatadog root.
- Note: because the `build_for_node` feature flag is not present, the node napi_rs specific `send_trace_node` will not be included, which if included would break the compiled dynamic library.
- Alternatively if you're on an M1 Mac and you want to cross-compile for linux x64, run the `cross_compile_linux_x64.sh` script.