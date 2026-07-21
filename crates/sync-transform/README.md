# sync-transform

First-class transform pipeline for surreal-sync (in-place stages, external child-stdio workers, ordered apply / watermark advance).

## Test gate

Run the full crate suite (unit + `external_stdio` fixture-worker integration + test-support harness):

```bash
cargo test -p sync-transform --all-features
```

`--all-features` enables `test-support`, which is required to build the `external_stdio` integration test and to export harness doubles used by apply-loop tests.

CI archives/runs the workspace with `--all-features` so `external_stdio` is included in nextest.
