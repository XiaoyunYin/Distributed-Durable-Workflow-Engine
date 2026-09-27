#!/usr/bin/env python3
"""Documented-shape AWS CLI stub for the read-only DUR-050 inventory tool."""
import json
import os
import sys


def tagged(resource_id_key: str, resource_id: str) -> dict:
    return {resource_id_key: resource_id, "Tags": [{"Key": "Task", "Value": "DUR-050"}]}


def main() -> int:
    args = sys.argv[1:]
    scenario = os.environ.get("DUR050_INVENTORY_SCENARIO", "empty")
    if os.environ.get("DUR050_INVENTORY_AWS_LOG"):
        with open(os.environ["DUR050_INVENTORY_AWS_LOG"], "a", encoding="utf-8") as stream:
            stream.write(json.dumps(args) + "\n")
    try:
        region_index = args.index("--region")
        region = args[region_index + 1]
        service = args[region_index + 2]
        operation = args[region_index + 3]
    except (ValueError, IndexError):
        print(json.dumps({"error": "unexpected argv", "argv": args}), file=sys.stderr)
        return 90
    if region != "us-west-1":
        print(json.dumps({"error": "unexpected region", "region": region}), file=sys.stderr)
        return 91

    if service == "sts" and operation == "get-caller-identity":
        account = "000000000000" if scenario == "wrong-account" else "372206265946"
        result = {"UserId": "AIDATESTUSER", "Account": account, "Arn": f"arn:aws:iam::{account}:user/test-inventory"}
    elif service == "ec2" and operation == "describe-instances":
        if scenario == "instance":
            result = {"Reservations": [{"Instances": [{"InstanceId": "i-0123456789abcdef0", "InstanceType": "c7i.large", "State": {"Name": "stopped"}, "Tags": [{"Key": "Task", "Value": "DUR-050"}]}]}]}
        else:
            result = {"Reservations": None if scenario == "null" else []}
    elif service == "ec2" and operation == "describe-volumes":
        if scenario == "volume":
            result = {"Volumes": [{"VolumeId": "vol-0123456789abcdef0", "State": "available", "Tags": []}]}
        else:
            result = {"Volumes": None if scenario == "null" else []}
    elif service == "ec2" and operation in {"describe-vpcs", "describe-subnets", "describe-security-groups", "describe-network-interfaces", "describe-addresses", "describe-snapshots", "describe-nat-gateways"}:
        config = {
            "describe-vpcs": ("vpc", "Vpcs", "VpcId", "vpc-0123456789abcdef0"),
            "describe-subnets": ("subnet", "Subnets", "SubnetId", "subnet-0123456789abcdef0"),
            "describe-security-groups": ("security-group", "SecurityGroups", "GroupId", "sg-0123456789abcdef0"),
            "describe-network-interfaces": ("eni", "NetworkInterfaces", "NetworkInterfaceId", "eni-0123456789abcdef0"),
            "describe-addresses": ("eip", "Addresses", "AllocationId", "eipalloc-0123456789abcdef0"),
            "describe-snapshots": ("snapshot", "Snapshots", "SnapshotId", "snap-0123456789abcdef0"),
            "describe-nat-gateways": ("nat", "NatGateways", "NatGatewayId", "nat-0123456789abcdef0"),
        }
        key, list_key, id_key, resource_id = config[operation]
        is_nat_scenario = key == "nat" and scenario == "nat"
        if scenario == key or is_nat_scenario:
            result = {list_key: [tagged(id_key, resource_id)]}
        else:
            result = {list_key: None if scenario == "null" else []}
    elif service == "elbv2" and operation == "describe-load-balancers":
        rows = [{"LoadBalancerArn": "arn:aws:elasticloadbalancing:us-west-1:000000000000:loadbalancer/app/dur050-test/abc"}] if scenario == "elbv2-lb" else []
        result = {"LoadBalancers": rows}
    elif service == "elbv2" and operation == "describe-tags":
        result = {"TagDescriptions": [{"ResourceArn": "arn:aws:elasticloadbalancing:us-west-1:000000000000:loadbalancer/app/dur050-test/abc", "Tags": [{"Key": "Task", "Value": "DUR-050"}]}] if scenario == "elbv2-lb" else []}
    elif service == "elb" and operation == "describe-load-balancers":
        result = {"LoadBalancerDescriptions": [{"LoadBalancerName": "durable-engine-dur050-test"}] if scenario == "classic-lb" else []}
    elif service == "elb" and operation == "describe-tags":
        result = {"TagDescriptions": [{"LoadBalancerName": "durable-engine-dur050-test", "Tags": [{"Key": "Task", "Value": "DUR-050"}]}] if scenario == "classic-lb" else []}
    elif service == "iam" and operation == "list-roles":
        result = {"Roles": [{"RoleName": "durable-engine-dur050-app-ssm"}] if scenario == "iam-role" else []}
    elif service == "iam" and operation == "list-instance-profiles":
        result = {"InstanceProfiles": [{"InstanceProfileName": "durable-engine-dur050-app-ssm"}] if scenario == "iam-profile" else []}
    elif service == "ssm" and operation == "get-parameters":
        names_index = args.index("--names") + 1
        names = args[names_index : args.index("--output")]
        live_name = None
        if scenario == "ssm-observer":
            live_name = next((name for name in names if name.endswith("dur050-observer-password")), None)
        elif scenario == "ssm-postgres":
            live_name = next((name for name in names if name.endswith("postgres-password")), None)
        returned = [{"Name": live_name, "Type": "SecureString", "Value": "[redacted]"}] if live_name else []
        invalid = [name for name in names if name != live_name]
        result = {"Parameters": returned, "InvalidParameters": invalid}
    else:
        print(json.dumps({"error": "unexpected AWS command", "argv": args}), file=sys.stderr)
        return 92
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
