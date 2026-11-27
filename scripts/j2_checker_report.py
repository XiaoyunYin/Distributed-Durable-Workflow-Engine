"""Index checker JSON without materializing the full Trace or changing its bytes."""

import hashlib
import json
import mmap
import re
from pathlib import Path

SPACE = re.compile(rb"[ \n\t\r]*")
STRING = rb'"(?:[^"\\]++|\\.)*+"'
TOKEN = re.compile(STRING + rb"|[\[\]{}]", re.DOTALL)
ATOM = re.compile(rb"[^\[\]{}:, \t\n\r]+")
JOIN_TABLES = {
    "engine.activity_attempts",
    "engine.outbox",
    "engine.transition_history",
    "engine.workflow_executions",
}


class IntegrityError(ValueError):
    classification = "integrity_stop"


class ResourceEvidenceError(RuntimeError):
    classification = "invalid"


def require_checker_exit(status):
    if type(status) is not int:
        raise ResourceEvidenceError("missing locked checker exit status")
    if status != 0:
        raise IntegrityError(f"locked checker exited {status} (see checker stdout and stderr)")


def require_checker_pass(report):
    if (
        report.get("status") != "PASS"
        or report.get("valid") is not True
        or report.get("violations")
    ):
        raise IntegrityError("locked checker failed structural checks")


def space(data, offset):
    return SPACE.match(data, offset).end()


def value_end(data, start):
    if data[start : start + 1] not in (b"{", b"["):
        token = (TOKEN if data[start : start + 1] == b'"' else ATOM).match(data, start)
        if token is None:
            raise ValueError("missing checker JSON value")
        return token.end()
    close = {ord("{"): ord("}"), ord("["): ord("]")}
    stack = []
    for token in TOKEN.finditer(data, start):
        symbol = data[token.start()]
        if symbol in close:
            stack.append(close[symbol])
        elif symbol != ord('"'):
            if stack.pop() != symbol:
                raise ValueError("unbalanced checker JSON")
            if not stack:
                return token.end()
    raise ValueError("truncated checker JSON")


def object_spans(data, start, visitors=None):
    if data[start : start + 1] != b"{":
        raise ValueError("expected checker JSON object")
    spans = {}
    offset = space(data, start + 1)
    if data[offset : offset + 1] == b"}":
        return spans, offset + 1
    while data[offset : offset + 1] == b'"':
        end = value_end(data, offset)
        key = json.loads(data[offset:end])
        colon = space(data, end)
        if key in spans or data[colon : colon + 1] != b":":
            raise ValueError(f"invalid or duplicate checker member: {key}")
        first = space(data, colon + 1)
        visitor = visitors(key) if visitors else None
        last = visitor(first) if visitor else value_end(data, first)
        spans[key] = (first, last)
        offset = space(data, last)
        delim = data[offset : offset + 1]
        if delim == b"}":
            return spans, offset + 1
        if delim != b",":
            break
        offset = space(data, offset + 1)
    raise ValueError("incomplete checker JSON object")


def indexed_report(
    path: Path, *, snapshot=None, join_trace=False, verify_tables=False, drain_counts=False
):
    """Return verdict fields and table inventory, with an optional replay projection."""
    tables = {}
    trace = {}
    with path.open("rb") as stream, mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as data:

        def load(span):
            return json.loads(data[span[0] : span[1]])

        def table(name):
            def visit(start):
                fields, end = object_spans(data, start)
                first, last = fields["rows"]
                tables[name] = {
                    "count": load(fields["count"]),
                    "sha256": load(fields["sha256"]),
                    "bytes": last - first,
                }
                if verify_tables:
                    with memoryview(data)[first:last] as rows:
                        digest = hashlib.sha256(rows).hexdigest()
                    if digest != tables[name]["sha256"]:
                        raise ValueError(f"snapshot table hash differs: {name}")
                if join_trace and name in JOIN_TABLES:
                    rows = load((first, last))
                    if name == "engine.outbox":
                        if drain_counts:
                            trace["UnpublishedOutbox"] = sum(
                                row["published_at"] is None for row in rows
                            )
                        rows = [row for row in rows if row["event_type"] == "activity.result"]
                    trace["Snapshot"][name] = {"rows": rows}
                return end

            return visit

        def visit_tables(start):
            trace["Snapshot"] = {}
            return object_spans(data, start, table)[1]

        def visit_trace(start):
            fields, end = object_spans(
                data, start, lambda name: visit_tables if name == "Snapshot" else None
            )
            if join_trace:
                trace["Submissions"] = load(fields["Submissions"])
            return end

        fields, end = object_spans(
            data, space(data, 0), lambda k: visit_trace if k == "trace" else None
        )
        if space(data, end) != len(data) or "trace" not in fields:
            raise ValueError("missing trace or trailing checker JSON")
        if snapshot is not None:
            start, stop = fields["trace"]
            with snapshot.open("wb") as output:
                for offset in range(start, stop, 1 << 20):
                    output.write(data[offset : min(stop, offset + (1 << 20))])
                output.write(b"\n")
        report = {name: load(span) for name, span in fields.items() if name != "trace"}
    return {**report, "trace": trace}, tables


def markers(path, prefix):
    with path.open(encoding="utf-8") as stream:
        return [
            json.loads(line.removeprefix(prefix + " "))
            for line in stream
            if line.startswith(prefix + " ")
        ]
