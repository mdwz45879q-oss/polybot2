// Criterion benchmarks are not used — the PyO3 cdylib crate type prevents
// rlib linking on macOS. Benchmarks are implemented as in-crate tests in
// src/bench_support.rs, gated behind the `bench-support` feature.
//
// Run with:
//   cargo test --manifest-path native/polybot2_native/Cargo.toml \
//     --no-default-features --features bench-support --release \
//     -- bench_ --nocapture
//
// On macOS, prepend: DYLD_LIBRARY_PATH=/path/to/python/lib

fn main() {
    println!("Benchmarks are implemented as in-crate tests under --features bench-support.");
    println!();
    println!("Run with:");
    println!("  cargo test --manifest-path native/polybot2_native/Cargo.toml \\");
    println!("    --no-default-features --features bench-support --release \\");
    println!("    -- bench_ --nocapture");
}
