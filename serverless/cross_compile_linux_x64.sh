#!/bin/bash

# run this script to cross compile dynamic library for linux x64

# musl is a version of the C standard library that can be statically linked
rustup target add x86_64-unknown-linux-musl

# see https://github.com/messense/homebrew-macos-cross-toolchains
brew tap messense/macos-cross-toolchains
brew install x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
echo "export CC_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-gcc
export CXX_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-g++
export AR_x86_64_unknown_linux_gnu=x86_64-unknown-linux-gnu-ar
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-gcc" \
    >> ~/.bashrc
source ~/.bashrc

# add linker to ~/.cargo/config if it doesn't already exist
grep -qxF "[target.x86_64-unknown-linux-musl]
linker = \"x86_64-unknown-linux-gnu-gcc\"" ~/.cargo/config || \
echo "[target.x86_64-unknown-linux-musl]
linker = \"x86_64-unknown-linux-gnu-gcc\"" >> ~/.cargo/config

# ---
# tooling setup is done
cargo build --release --target x86_64-unknown-linux-gnu