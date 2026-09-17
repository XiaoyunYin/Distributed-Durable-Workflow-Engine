# Workflow partition map v1

The initial scheduler topology has 16 logical partitions. The mapping is
stable across supported implementations and does not use a language/runtime
hash. For a non-empty UTF-8 workflow ID:

```text
digest = SHA-256(UTF-8(workflow_id))
value  = unsigned_big_endian_u64(digest[0:8])
partition = value modulo 16
```

Map version: `sha256-u64-be-v1`.

The canonical vectors live in `api/partition-map-v1.vectors.json` and are
loaded independently by the Go and Python tests. The file includes the
non-ASCII vector `wf-é-日本`; invalid UTF-8 is rejected by both APIs and is
tested separately because it cannot be represented in UTF-8 JSON. The empty ID
is invalid.
