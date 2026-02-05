# CLOB Ledger

Ledger service responsible for validating funds and publishing order commands to the matching engine.

## Responsibilities
- Consumes `ledger.requests` (protobuf `InputEvent`)
- Debits balances in Postgres
- Publishes `ledger.events` (JSON)
- Publishes `clob.inputs` (protobuf `InputEvent`)
- Uses an outbox table for reliable publishing
- Optional reconciler validates that engine fills are finalized in Postgres

## Env vars
- `NATS_URL` (default: `nats://127.0.0.1:4222`)
- `LEDGER_STREAM_NAME` (default: `CLOB`)
- `LEDGER_REQUESTS_SUBJECT` (default: `ledger.requests`)
- `LEDGER_EVENTS_SUBJECT` (default: `ledger.events`)
- `ENGINE_INPUT_SUBJECT` (default: `clob.inputs`)
- `ENGINE_OUTPUT_SUBJECT` (default: `clob.outputs`)
- `LEDGER_CONSUMER` (default: `ledger-requests`)
- `LEDGER_OUTPUTS_CONSUMER` (default: `ledger-outputs`)
- `DATABASE_URL` (default: `postgres://postgres:postgres@127.0.0.1:5432/clob_ledger?sslmode=disable`)
- `LEDGER_MARKETS` (optional; format: `0x<64-hex>=BASE/QUOTE,0x<64-hex>=BASE/QUOTE`)
- `RECONCILER_CONSUMER` (default: `ledger-reconciler`)
- `RECONCILER_ALERTS_SUBJECT` (default: `ledger.reconcile.alerts`)
- `RECONCILER_GRACE_MS` (default: `2000`)
- `RECONCILER_ACK_ON_MISSING` (default: `true`)
- `LEDGER_HTTP_ADDR` (default: `127.0.0.1:8082`)
- `LEDGER_HTTP_READ_TIMEOUT` (default: `5s`)
- `LEDGER_HTTP_WRITE_TIMEOUT` (default: `10s`)
- `LEDGER_HTTP_SHUTDOWN_TIMEOUT` (default: `10s`)
- `LEDGER_HTTP_TOKEN` (optional; require `X-Internal-Token` header when set)

## Notes
- Balances are stored as an append-only `ledger_transactions` table (per asset). Quote asset balances are in 0.1 cent units.
- The ledger debits quote asset for bids and base asset for asks.
- Orders must provide a price in 0.1 cent ticks (1-1000), matching the USD base pair.
- If `LEDGER_MARKETS` is unset, all requests are rejected as `invalid_token_id`.

## HTTP API
All endpoints require an `account_id` query parameter. This API is intended for internal use.

- `GET /v1/balance?account_id=...&asset=...` -> latest balance for one asset
- `GET /v1/balances?account_id=...` -> latest balance for all assets
- `GET /v1/orders?account_id=...&token_id=...&status=...&limit=...` -> historical orders
- `GET /v1/fills?account_id=...&token_id=...&limit=...` -> historical fills

Status filter accepts `pending`, `filled` (alias: `matched`), `canceled`, `rejected`.
