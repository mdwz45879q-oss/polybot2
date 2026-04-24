# polybot2_native

Rust extension module for the native MLB hotpath engine.

## Build

From `/Users/reda/polymarket_bot/polybot2`:

```bash
maturin develop --manifest-path native/polybot2_native/Cargo.toml
```

or build a wheel:

```bash
maturin build --manifest-path native/polybot2_native/Cargo.toml
```

`polybot2 hotpath run` and runtime benchmark paths require this module.
