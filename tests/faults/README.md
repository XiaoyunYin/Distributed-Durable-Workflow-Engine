# Fault tests

DUR-003's event-driven named-boundary fixture lives in
`tests/test_fault_control.py` and `python/faults/`. The reusable Python and Go
clients read the controller endpoint from the environment. The controller can
launch an arbitrary command; the bundled target is a seeded fake activity that
reports a named boundary, blocks until an explicit release or kill command,
and writes a versioned JSONL trace. `pause()` records that the target remains
held at the already-reported barrier; it does not send a second blocking
command. stdout/stderr are separate drained logs,
and early process exit is distinct from a boundary timeout.

Run it with the normal suite:

```powershell
uv run pytest tests/test_fault_control.py
```
