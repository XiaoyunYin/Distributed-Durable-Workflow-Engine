# Scripted local demo

`recruiter-demo.tape` records a real HTTP submission through the running local
Compose stack. The helper creates a temporary workflow definition, submits the
workflow through the API, waits for Python-worker results and scheduler
consumption, and checks persisted history with the independent invariant
checker. Before cleanup, it waits until every workflow outbox row is published,
has a durable inbox record, and the corresponding Kafka consumer-group offset
is committed past that record. It then removes the inbox records, workflow and
definition in one transaction; if the demo fails before that safe point, it
leaves the durable rows available for diagnosis.

## Render

Start the local stack first, for example with
`powershell -NoProfile -ExecutionPolicy Bypass -File scripts/bootstrap.ps1 -StartServices`.
Keep the default Compose service names and the runtime API on port 8080. Then run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/render-recruiter-demo.ps1
```

The renderer cross-compiles the Go helper for Linux and builds the pinned VHS
v0.12.0 source with a minimal render-context correction: upstream cancels the
recording context before its deferred FFmpeg render, which can otherwise exit
successfully without creating a GIF. It then runs the patched VHS binary in the
pinned container on the same Compose network as PostgreSQL and `runtime-a`.
The database URL is passed through a temporary ignored env file and is never
typed into the recording. The script verifies the GIF exists and is under two
minutes. The generated `recruiter-demo.gif` is the tape output, not a manually
edited screen capture. Requirements: Go 1.26.7 or later, Docker with the Linux
engine, the local Compose services, and the generated `.env`.

The captured run is a bounded local happy-path demonstration, not a recovery
drill or a performance measurement. The separately linked failure case study
is the recovery example.
