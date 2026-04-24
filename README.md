# polybot2

Standalone Polymarket + provider deterministic linking runtime.

## Install

```bash
pip install -e /Users/reda/polymarket_bot/polybot2
```

## CLI

```bash
polybot2 data sync --markets
polybot2 provider sync
polybot2 mapping validate
polybot2 link build --league-scope live
polybot2 link report
```

## Native Hotpath Exec Payload (Current)

Native Rust runtime now accepts a strict market-only execution payload surface:

- `dispatch_mode`
- `clob_host`
- `api_key`
- `api_secret`
- `api_passphrase`
- `funder`
- `signature_type`
- `address`
- `chain_id`
- `presign_enabled`
- `presign_private_key`
- `presign_pool_target_per_key`
- `presign_refill_batch_size`
- `presign_refill_interval_seconds`
- `presign_startup_warm_timeout_seconds`
- `active_order_refresh_interval_seconds`

Deprecated native hotpath keys were hard-removed and now fail at the native boundary.
