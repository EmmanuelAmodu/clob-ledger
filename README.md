# CLOB Ledger

Ledger service responsible for validating funds and publishing order commands to the matching engine.

## Responsibilities
- Consumes `ledger.requests` (protobuf `InputEvent`)
- Debits balances in Postgres
- Publishes `ledger.events` (JSON)
- Publishes `clob.inputs` (protobuf `InputEvent`)
- Uses an outbox table for reliable publishing

## Env vars
- `NATS_URL` (default: `nats://127.0.0.1:4222`)
- `LEDGER_STREAM_NAME` (default: `CLOB`)
- `LEDGER_REQUESTS_SUBJECT` (default: `ledger.requests`)
- `LEDGER_EVENTS_SUBJECT` (default: `ledger.events`)
- `ENGINE_INPUT_SUBJECT` (default: `clob.inputs`)
- `LEDGER_CONSUMER` (default: `ledger-requests`)
- `DATABASE_URL` (default: `postgres://postgres:postgres@127.0.0.1:5432/clob_ledger?sslmode=disable`)
- `LEDGER_MARKETS` (optional; format: `token_id=BASE/QUOTE,token_id=BASE/QUOTE`)

## Notes
- Balances are stored as an append-only `ledger_transactions` table (per asset).
- The ledger debits quote asset for bids and base asset for asks.
- Orders must provide a price in cents (1-99), matching the USD base pair.
- If `LEDGER_MARKETS` is unset, all requests are rejected as `invalid_token_id`.
