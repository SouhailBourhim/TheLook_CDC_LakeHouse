# Data contracts (ADR 020)

One [ODCS](https://bitol-io.github.io/open-data-contract-standard/) v3 file per
table: `contracts/<layer>/<table>.odcs.yaml`. They are the single source of
truth for what each table must look like (spec FR7).

| Layer | Contracts | What they check |
|---|---|---|
| `silver/` | 7 | schema, primary key, required fields, PII tags, quality rules (accepted statuses, value ranges, timestamps that must follow a status) |
| `gold/` | 10 | the same, plus relationships (facts to dimensions), metric identities (net = gross - returns) and the unknown member rule |
| `bronze/` | 8 | schema only (7 change-event tables and the batch ledger), to catch drift |

Drafted from the Glue catalog with `datacontract import glue`, then curated by
hand. The draft is never committed as is: it holds the AWS account ID.

**Checks**

- `datacontract lint <file>`: the file is valid ODCS (CI, no AWS).
- `datacontract test <file>`: runs every schema and quality check in Athena
  (workgroup `thelook`). Version 1.2.2 needs the server's `stagingDir`; it
  holds the account ID, so the runner adds it at run time.
- `python drift.py */*.odcs.yaml`: each contract's fields vs its table's
  columns in Glue. datacontract-cli ignores columns a contract does not
  list, so a new column (possibly personal data) would otherwise pass.

When a table gains a column on purpose: add it to the contract (with its
PII tag if it has one) in the same change.
