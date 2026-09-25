# Pilot calibration inputs and evidence

This directory is reserved for cycle-independent calibration inputs and
calibration data. `frozen-config.json` is the committed, cycle-independent
load-generator template: the preparation tool renders `api_url` and the
`{cycle_id}` namespace into a separate file under `cycles/<cycle-id>/` and
records that file's SHA-256. Never edit it to insert an ephemeral host address.

No unloaded calibration rows, gate-overhead rows, sink result, knee staircase,
or numeric SLO have been produced yet. The cycle-2 fixture and baseline setup
failed before warmup or measurement; its operational records live in
`../cycles/cycle-2/` and are not calibration results.

Any future rows placed here must be labelled **CALIBRATION — NOT RESULTS** and
must retain their raw source blocks. Operational, preflight, failure, and cost
ledger records belong under their provisioning-cycle directory.
