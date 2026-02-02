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

## Notes
- Balances are stored as an append-only `ledger_transactions` table (per asset).
- The ledger debits quote asset for bids and base asset for asks.
- Orders must provide a price in cents (1-99), matching the USD base pair.
- If `LEDGER_MARKETS` is unset, all requests are rejected as `invalid_token_id`.
