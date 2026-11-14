"""Regenerate the executable K4 closed field reference without editing prose."""

from pathlib import Path

import dur053_k4_schema as schema

START = "## Complete closed field reference\n"
END = "## Round 239 binding Phase B2 additions\n"
NAMES = [
    "IDENTITY",
    "MEMBER",
    "SESSION",
    "INVOCATION",
    "COMMIT_BRACKET",
    "CLAIM",
    "HISTORY",
    "RECEIPT",
    "WORKFLOW",
    "HTTP",
    "WITNESS",
    "LEASE",
    "CALLBACK",
    "ARRIVAL",
    "SAMPLE",
    "OVERLAP",
    "EVICTION",
    "TARGET",
    "CONFIRMATION",
    "HOLD_STATE",
    "HOLD",
    "EPISODE",
    "DISPOSITION",
    "JOB",
    "SAFETY",
]


def type_name(spec, *, nested=True):
    if nested:
        for name in NAMES:
            if spec is getattr(schema, name):
                return name
    if isinstance(spec, dict):
        return "{" + ", ".join(k + ": " + type_name(v) for k, v in spec.items()) + "}"
    if isinstance(spec, list):
        return "[" + type_name(spec[0]) + "]"
    if isinstance(spec, tuple):
        return type_name(spec[0]) + "?"
    if isinstance(spec, frozenset):
        return " | ".join(repr(s) for s in sorted(spec))
    return {str: "string", int: "integer", bool: "boolean"}.get(spec, str(spec))


def reference():
    text = (
        START
        + "\nThe following generated type reference mirrors the executable SCHEMAS registry.\n"
    )
    text += "Every object listed is closed. `T?` permits explicit null, `[T]` is an array,\nand enum alternatives are exact strings.\n\n"
    objects = [(name, getattr(schema, name)) for name in NAMES] + list(schema.SCHEMAS.items())
    for name, spec in objects:
        text += "### " + name + "\n\n| Required field | Type |\n| --- | --- |\n"
        for key, value in spec.items():
            text += "| `" + key + "` | `" + type_name(value).replace("|", "\\|") + "` |\n"
        text += "\n"
    return text


def document(text):
    before, rest = text.split(START, 1)
    _, after = rest.split(END, 1)
    return before + reference() + END + after


if __name__ == "__main__":
    path = Path(__file__).resolve().parents[1] / "experiments/portfolio/k8s-recovery/k4-schema.md"
    path.write_bytes(document(path.read_text(encoding="utf-8")).encode())
