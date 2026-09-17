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

| Workflow ID | Partition |
| --- | ---: |
| `workflow-0001` | 8 |
| `workflow-0002` | 14 |
| `incident-2026-09-14-a` | 0 |
| `retry-key/abc` | 0 |

The empty ID is invalid. The Go and Python implementations and their tests
are the executable cross-implementation evidence for these vectors.
