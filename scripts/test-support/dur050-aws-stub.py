#!/usr/bin/env python3
"""CI-only AWS/SSM double; SSM commands are executed through /bin/sh (dash)."""
from __future__ import annotations

import json
import os
import pathlib
import re
import subprocess
import sys

args = sys.argv[1:]
if len(args) >= 2 and args[0] == "--region":
    args = args[2:]
scenario = os.environ.get("DUR050_AWS_SCENARIO", "pass")
state = pathlib.Path(os.environ["DUR050_AWS_STATE"])
state.mkdir(parents=True, exist_ok=True)
if os.environ.get("DUR050_ARGV_LOG"):
    with open(os.environ["DUR050_ARGV_LOG"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps(["aws", *args]) + "\n")


def emit(value: object) -> None:
    print(json.dumps(value))


if args[:2] == ["sts", "get-caller-identity"]:
    account = "000000000000" if scenario == "account" else "372206265946"
    emit({"Account": account, "Arn": f"arn:aws:iam::{account}:user/ci-stub"})
elif args[:2] == ["service-quotas", "get-service-quota"]:
    emit({"Quota": {"QuotaCode": "L-1216C47A", "ServiceCode": "ec2", "Value": 4 if scenario == "quota" else 32}})
elif args[:2] == ["ec2", "describe-instances"]:
    emit({"Reservations": []})
elif args[:2] == ["budgets", "describe-budget"]:
    emit({"Budget": {"BudgetLimit": {"Amount": "200", "Unit": "USD"}, "CalculatedSpend": {"ActualSpend": {"Amount": "1"}, "ForecastedSpend": {"Amount": "5"}}}})
elif args[:2] == ["budgets", "describe-notifications-for-budget"]:
    rows = [
        {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 160, "ThresholdType": "ABSOLUTE_VALUE", "NotificationState": "OK"},
        {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 76.47, "ThresholdType": "ABSOLUTE_VALUE", "NotificationState": "OK"},
        {"NotificationType": "FORECASTED", "ComparisonOperator": "GREATER_THAN", "Threshold": 160, "ThresholdType": "ABSOLUTE_VALUE", "NotificationState": "OK"},
    ]
    emit({"Notifications": rows[:2] if scenario == "budget" else rows})
elif args[:2] == ["ce", "list-cost-allocation-tags"]:
    status = "Inactive" if scenario == "tags" else "Active"
    emit({"CostAllocationTags": [{"TagKey": "Task", "Status": status, "LastUpdatedDate": "2026-09-25T07:06:39Z"}, {"TagKey": "Environment", "Status": "Active", "LastUpdatedDate": "2026-09-25T07:06:39Z"}]})
elif args[:2] == ["ssm", "get-parameter"]:
    print("ci-observer-password")
elif args[:2] == ["ssm", "send-command"]:
    parameter = args[args.index("--cli-input-json") + 1]
    request = json.loads(pathlib.Path(parameter.removeprefix("file://")).read_text(encoding="utf-8"))
    count_file = state / "count"
    count = int(count_file.read_text() or "0") + 1 if count_file.exists() else 1
    count_file.write_text(str(count))
    command_id = f"ci-{count}"
    command = request["Parameters"]["commands"][0]
    completed = subprocess.run(["/bin/sh", "-c", command], capture_output=True, text=True, env=os.environ.copy())
    fail_match = os.environ.get("DUR050_AWS_FAIL_STAGE", "")
    status = "Success" if completed.returncode == 0 else "Failed"
    if fail_match and fail_match in request.get("Comment", ""):
        status = "Failed"
    result = {"Status": status, "StandardOutputContent": completed.stdout, "StandardErrorContent": completed.stderr}
    (state / f"{command_id}.json").write_text(json.dumps(result), encoding="utf-8")
    emit({"Command": {"CommandId": command_id}})
elif args[:2] == ["ssm", "get-command-invocation"]:
    command_id = args[args.index("--command-id") + 1]
    result_path = state / f"{command_id}.json"
    if not result_path.exists():
        print("missing SSM stub result", file=sys.stderr)
        raise SystemExit(91)
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if scenario == "bootstrap" and "cycle preflight" in " ".join(args):
        result["Status"] = "Failed"
        result["StandardErrorContent"] = "injected bootstrap gate failure"
    emit(result)
else:
    print(f"unexpected AWS stub argv: {args!r}", file=sys.stderr)
    raise SystemExit(90)
