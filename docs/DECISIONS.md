# Architecture and scope decisions

## D001 - DUR-001 development pins

- Date: 2026-09-14
- Status: accepted for local development

Pin Go 1.27.1, CPython 3.12.7, PostgreSQL 18.6, Apache Kafka 4.3.1,
OpenTelemetry Collector Contrib 0.160.0, and Prometheus 3.13.0 LTS.
Python development dependencies are resolved exactly in `uv.lock`; container
base and service images use explicit version tags plus pulled manifest digests
rather than floating or mutable tag-only references.

Go 1.27.1 and PostgreSQL/Kafka versions were selected after checking their
official release sources on 2026-09-14. Python 3.12.7 and uv 0.9.5 match the
already installed host tools and remain within an intentionally narrow Python
compatibility range. Prometheus uses the supported LTS line rather than the
fast-moving latest line. The collector uses the most recently confirmed
released version rather than a scheduled same-day version.

Reconsider these pins for security releases, unsupported versions, or before a
frozen benchmark protocol. A version change requires rerunning clean bootstrap
and restart-smoke evidence; it does not change a protected runtime guarantee.

## D002 - Single-node local dependencies

- Date: 2026-09-14
- Status: accepted for DUR-001 development only

Use one PostgreSQL container and one combined-mode KRaft Kafka container with
named volumes. This is the minimum topology that exercises the real database
and broker locally. It is not evidence of database replication, Kafka broker
failover, host-level availability, or final Linux I/O performance.

The alternative of mocks or embedded substitutes was rejected because the plan
requires real PostgreSQL and Kafka. A multi-node broker/database deployment is
unnecessary for the foundation task and would imply guarantees not yet tested.

## D003 - Versioned stable partition mapping

- Date: 2026-09-16
- Status: accepted for DUR-002

Use `sha256-u64-be-v1`: SHA-256 of the UTF-8 workflow ID, read as an unsigned
big-endian 64-bit integer from the first eight digest bytes, reduced modulo 16.
The map version and partition count are part of the durable workflow identity
contract. Go and Python keep independent implementations and tests so a
runtime-specific hash cannot silently change ownership after a restart.

## D004 - Named-boundary fault control and shared checks

- Date: 2026-09-16
- Status: accepted for M0

Fault fixtures report named boundaries over a process pipe and block until the
controller sends an explicit release or kills the target. The controller uses
event waits with bounded timeouts; it does not infer a boundary from sleeps.
Trace records use `fault-trace.v1` and carry the seed and sequence number.

`scripts/ci.ps1` composes the existing checks and exposes race and real-service
checks as explicit switches. Paid/model checks remain outside the foundation
entry point. This is a local/shared validation contract; no remote CI service
is claimed when the repository has no configured remote.
