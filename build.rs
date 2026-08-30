// SPDX-License-Identifier: MIT OR Apache-2.0
//! With the `jit` feature, revmc's AOT artifacts are shared libraries that
//! call back into the builtins linked into this binary, so those symbols
//! have to be in its dynamic symbol table.

fn main() {
    if std::env::var_os("CARGO_FEATURE_JIT").is_some() {
        #[cfg(feature = "jit")]
        revmc_build::emit();
    }
    println!("cargo:rerun-if-changed=build.rs");
}
