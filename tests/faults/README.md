# Fault tests

DUR-003's event-driven named-boundary fixture lives in
`tests/test_fault_control.py` and `python/faults/`. The target reports a
named boundary, blocks until an explicit release or kill command, and writes a
versioned JSONL trace. The timeout case is controlled by an event wait rather
than a sleep-based boundary guess.

Run it with the normal suite:

```powershell
uv run pytest tests/test_fault_control.py
```
