"""SQLite-backed, approval-gated incident investigation workflow."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol, cast

from incident_agent.fixtures import build_incident_cases
from incident_agent.mcp import BoundedMCPServer
from incident_agent.models import Proposal, TimelineEvent, WorkflowSnapshot, WorkflowState
from incident_agent.redaction import redact


class WorkflowError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiveModelConfig:
    """Explicit budget gate for nondeterministic provider adapters."""

    provider: str
    model: str
    budget_cents: int

    def authorize(self) -> None:
        if self.budget_cents <= 0:
            raise WorkflowError("live model mode requires a positive approved budget")
        if os.environ.get("INCIDENT_LIVE_APPROVED") != "1":
            raise WorkflowError("live model mode requires INCIDENT_LIVE_APPROVED=1")


class DurableStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS incidents (
                run_id TEXT PRIMARY KEY, case_id TEXT NOT NULL, state TEXT NOT NULL,
                revision INTEGER NOT NULL, diagnosis TEXT, citations TEXT NOT NULL,
                proposal TEXT, receipt TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS timeline (
                run_id TEXT NOT NULL, sequence INTEGER NOT NULL, event_type TEXT NOT NULL,
                actor TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY (run_id, sequence),
                FOREIGN KEY (run_id) REFERENCES incidents(run_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS approvals (
                run_id TEXT PRIMARY KEY, decision TEXT NOT NULL, actor TEXT, decided_at REAL,
                FOREIGN KEY (run_id) REFERENCES incidents(run_id) ON DELETE CASCADE
            );
            """
        )

    def close(self) -> None:
        self.connection.close()

    def create(self, run_id: str, case_id: str) -> None:
        now = time.time()
        self.connection.execute(
            "INSERT OR IGNORE INTO incidents VALUES "
            "(?, ?, 'ACCEPTED', 0, NULL, '[]', NULL, NULL, ?, ?)",
            (run_id, case_id, now, now),
        )
        self.connection.commit()

    def row(self, run_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM incidents WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise WorkflowError(f"unknown incident run: {run_id}")
        return cast(sqlite3.Row, row)

    def event(
        self, run_id: str, event_type: str, actor: str, data: dict[str, Any]
    ) -> TimelineEvent:
        row = self.row(run_id)
        sequence = int(
            self.connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 FROM timeline WHERE run_id = ?", (run_id,)
            ).fetchone()[0]
        )
        event = TimelineEvent(sequence, event_type, actor, data)
        self.connection.execute(
            "INSERT INTO timeline VALUES (?, ?, ?, ?, ?)",
            (run_id, sequence, event_type, actor, json.dumps(data, sort_keys=True)),
        )
        self.connection.execute(
            "UPDATE incidents SET revision = ?, updated_at = ? WHERE run_id = ?",
            (int(row["revision"]) + 1, time.time(), run_id),
        )
        self.connection.commit()
        return event

    def update(
        self,
        run_id: str,
        state: WorkflowState,
        diagnosis: str | None = None,
        citations: tuple[str, ...] | None = None,
        proposal: Proposal | None = None,
        receipt: dict[str, Any] | None = None,
    ) -> None:
        current = self.row(run_id)
        self.connection.execute(
            "UPDATE incidents SET state = ?, diagnosis = COALESCE(?, diagnosis), "
            "citations = ?, proposal = ?, receipt = ?, updated_at = ? WHERE run_id = ?",
            (
                state,
                diagnosis,
                json.dumps(
                    citations if citations is not None else json.loads(current["citations"])
                ),
                json.dumps(asdict(proposal)) if proposal is not None else current["proposal"],
                json.dumps(receipt) if receipt is not None else current["receipt"],
                time.time(),
                run_id,
            ),
        )
        self.connection.commit()

    def approve(self, run_id: str, decision: str, actor: str) -> None:
        self.connection.execute(
            "INSERT OR REPLACE INTO approvals VALUES (?, ?, ?, ?)",
            (run_id, decision, actor, time.time()),
        )
        self.connection.commit()

    def approval(self, run_id: str) -> str | None:
        row = self.connection.execute(
            "SELECT decision FROM approvals WHERE run_id = ?", (run_id,)
        ).fetchone()
        return None if row is None else str(row[0])

    def timeline(self, run_id: str) -> tuple[TimelineEvent, ...]:
        rows = self.connection.execute(
            "SELECT sequence, event_type, actor, data FROM timeline "
            "WHERE run_id = ? ORDER BY sequence",
            (run_id,),
        ).fetchall()
        return tuple(
            TimelineEvent(int(row[0]), str(row[1]), str(row[2]), json.loads(row[3])) for row in rows
        )


class DecisionProvider(Protocol):
    def diagnose(
        self, case_id: str, evidence_ids: tuple[str, ...], tool_text: str
    ) -> dict[str, Any]: ...


class FixtureDecisionProvider:
    """Recorded model decisions used by deterministic correctness runs."""

    def __init__(self) -> None:
        self.cases = {case.case_id: case for case in build_incident_cases()}

    def diagnose(
        self, case_id: str, evidence_ids: tuple[str, ...], tool_text: str
    ) -> dict[str, Any]:
        case = self.cases[case_id]
        proposal = None if not case.answerable else case.expected_action
        return {
            "diagnosis": case.expected_diagnosis,
            "citations": list(evidence_ids),
            "proposal": proposal,
            "uncertainty": not case.answerable,
        }


class SandboxEffect:
    def __init__(self, mcp: BoundedMCPServer) -> None:
        self.mcp = mcp
        self._receipts: dict[str, dict[str, Any]] = {}

    def apply(self, run_id: str, proposal: Proposal) -> dict[str, Any]:
        existing = self._receipts.get(proposal.canonical_argument_hash)
        if existing is not None:
            return existing
        receipt = {
            "resource_id": proposal.target_resource_id,
            "action": proposal.action_type,
            "state": "APPLIED",
            "argument_hash": proposal.canonical_argument_hash,
        }
        self._receipts[proposal.canonical_argument_hash] = receipt
        self.mcp.set_remediation_status(proposal.target_resource_id, "APPLIED")
        return receipt


def proposal_from_dict(value: dict[str, Any]) -> Proposal:
    action = str(value["action"])
    target = str(value.get("service", value.get("resource_id", "incident-resource")))
    arguments = {
        key: item for key, item in value.items() if key not in {"action", "service", "resource_id"}
    }
    canonical = json.dumps(
        {"action": action, "target": target, "arguments": arguments},
        sort_keys=True,
        separators=(",", ":"),
    )
    return Proposal(action, target, arguments, hashlib.sha256(canonical.encode()).hexdigest())


class InvestigationWorkflow:
    def __init__(
        self, store: DurableStore, mcp: BoundedMCPServer, decisions: DecisionProvider | None = None
    ) -> None:
        self.store = store
        self.mcp = mcp
        self.decisions = decisions or FixtureDecisionProvider()
        self.effect = SandboxEffect(mcp)

    def start(self, case_id: str, run_id: str | None = None, arm: str = "hybrid") -> str:
        if case_id not in {case.case_id for case in build_incident_cases()}:
            raise WorkflowError(f"unknown incident case: {case_id}")
        run_id = run_id or f"incident-{uuid.uuid4().hex}"
        self.store.create(run_id, case_id)
        if not self.store.timeline(run_id):
            self.store.event(
                run_id, "incident_accepted", "client", {"case_id": case_id, "arm": arm}
            )
        return run_id

    def investigate(
        self, run_id: str, arm: str = "hybrid", interrupt_after: str | None = None
    ) -> WorkflowSnapshot:
        row = self.store.row(run_id)
        case = next(case for case in build_incident_cases() if case.case_id == row["case_id"])
        if row["state"] in {"COMPLETED", "ABSTAINED", "REJECTED"}:
            return self.snapshot(run_id)
        self.store.update(run_id, "INVESTIGATING")
        self.store.event(run_id, "investigation_started", "scheduler", {"round": 1})
        logs = self.mcp.query_logs(run_id, case.case_id, case.service)
        self._record_tool_call(run_id, logs)
        if interrupt_after == "logs":
            self.store.update(run_id, "INTERRUPTED")
            self.store.event(run_id, "interrupted", "scheduler", {"boundary": "after_logs"})
            return self.snapshot(run_id)
        metrics = self.mcp.query_metrics(run_id, case.case_id, case.service)
        self._record_tool_call(run_id, metrics)
        search = self.mcp.search_runbooks(run_id, case.query, cast(Any, arm))
        self._record_tool_call(run_id, search)
        if not bool(search.data["sufficient"]):
            self.store.event(
                run_id,
                "investigation_round_opened",
                "scheduler",
                {"round": 2, "reason": search.data["reason"]},
            )
            search = self.mcp.search_runbooks(run_id, f"{case.query} runbook", cast(Any, arm))
            self._record_tool_call(run_id, search)
        evidence_ids = search.evidence_ids
        decision = self.decisions.diagnose(
            case.case_id,
            evidence_ids,
            json.dumps(
                {"logs": logs.data, "metrics": metrics.data, "search": search.data}, sort_keys=True
            ),
        )
        citations = tuple(str(item) for item in decision.get("citations", []))
        if any(citation not in evidence_ids for citation in citations):
            raise WorkflowError("diagnosis cited evidence not returned by an authorized tool")
        diagnosis = redact(str(decision.get("diagnosis", ""))).text
        self.store.event(
            run_id,
            "diagnosis_committed",
            "model-fixture",
            {
                "diagnosis": diagnosis,
                "citations": list(citations),
                "tool_call_count": len(self.mcp.calls(run_id)),
            },
        )
        proposal_value = decision.get("proposal")
        if not isinstance(proposal_value, dict):
            self.store.update(run_id, "ABSTAINED", diagnosis, citations)
            self.store.event(run_id, "abstained", "scheduler", {"reason": "no_safe_proposal"})
        else:
            if redact(json.dumps(proposal_value, sort_keys=True)).count:
                raise WorkflowError("proposal contains sensitive material")
            proposal = proposal_from_dict(proposal_value)
            self.store.update(run_id, "WAITING_APPROVAL", diagnosis, citations, proposal)
            self.store.event(
                run_id,
                "approval_requested",
                "scheduler",
                {
                    "proposal_hash": proposal.canonical_argument_hash,
                    "target": proposal.target_resource_id,
                },
            )
        return self.snapshot(run_id)

    def _record_tool_call(self, run_id: str, result: Any) -> None:
        call = self.mcp.calls(run_id)[-1]
        self.store.event(
            run_id,
            "mcp_tool_call",
            "mcp",
            {
                "method": result.method,
                "schema_version": result.schema_version,
                "evidence_ids": list(result.evidence_ids),
                "redactions": result.redactions,
                "latency_ms": call.latency_ms,
                "row_count": len(result.data.get("rows", result.data.get("evidence", []))),
            },
        )

    def approve(self, run_id: str, actor: str) -> WorkflowSnapshot:
        row = self.store.row(run_id)
        if row["state"] != "WAITING_APPROVAL":
            raise WorkflowError("approval is only valid while waiting for approval")
        self.store.approve(run_id, "APPROVED", actor)
        self.store.event(run_id, "approval_granted", actor, {})
        return self.resume(run_id)

    def reject(self, run_id: str, actor: str) -> WorkflowSnapshot:
        row = self.store.row(run_id)
        if row["state"] != "WAITING_APPROVAL":
            raise WorkflowError("rejection is only valid while waiting for approval")
        self.store.approve(run_id, "REJECTED", actor)
        self.store.update(run_id, "REJECTED")
        self.store.event(run_id, "approval_rejected", actor, {"no_action": True})
        return self.snapshot(run_id)

    def resume(self, run_id: str) -> WorkflowSnapshot:
        row = self.store.row(run_id)
        if row["state"] == "INTERRUPTED":
            return self.investigate(run_id)
        if row["state"] != "WAITING_APPROVAL" or self.store.approval(run_id) != "APPROVED":
            return self.snapshot(run_id)
        proposal_data = json.loads(row["proposal"])
        proposal = Proposal(**proposal_data)
        self.store.update(run_id, "DISPATCHED")
        self.store.event(
            run_id,
            "effect_dispatched",
            "scheduler",
            {"proposal_hash": proposal.canonical_argument_hash},
        )
        receipt = self.effect.apply(run_id, proposal)
        self.store.update(run_id, "VERIFYING", receipt=receipt)
        self.store.event(
            run_id, "effect_verified", "tool", {"resource_id": proposal.target_resource_id}
        )
        self.store.update(run_id, "COMPLETED", receipt=receipt)
        self.store.event(
            run_id, "incident_completed", "scheduler", {"action": proposal.action_type}
        )
        return self.snapshot(run_id)

    def snapshot(self, run_id: str) -> WorkflowSnapshot:
        row = self.store.row(run_id)
        proposal = Proposal(**json.loads(row["proposal"])) if row["proposal"] else None
        return WorkflowSnapshot(
            run_id=run_id,
            case_id=str(row["case_id"]),
            state=cast(WorkflowState, row["state"]),
            revision=int(row["revision"]),
            diagnosis=row["diagnosis"],
            citations=tuple(json.loads(row["citations"])),
            proposal=proposal,
            action_receipt=json.loads(row["receipt"]) if row["receipt"] else None,
            timeline=self.store.timeline(run_id),
        )
