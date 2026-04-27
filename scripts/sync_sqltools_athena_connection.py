#!/usr/bin/env python3
import argparse
import configparser
import json
import os
import subprocess
import urllib.parse
from pathlib import Path


def get_connection_id(conn: dict) -> str:
    if conn.get("id"):
        return str(conn["id"])
    parts = [conn.get("name"), conn.get("driver")]
    if conn.get("connectString"):
        parts.append(conn.get("connectString"))
    else:
        parts.extend([conn.get("server"), conn.get("database")])
    raw = "|".join("" if p is None else str(p) for p in parts)
    return raw.replace(".", ":").replace("/", "\\")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-file", required=True)
    parser.add_argument("--profile", default="cc-analytics")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--workgroup", default="primary")
    parser.add_argument(
        "--output-location",
        default="s3://data-stellar-athena-query-results/tmp/",
    )
    parser.add_argument("--connection-name", default="Athena (cc-analytics)")
    parser.add_argument(
        "--credentials-file",
        default=os.environ.get("AWS_CREDENTIALS_FILE", "~/.aws/credentials"),
    )
    parser.add_argument(
        "--creds-json",
        default=os.environ.get("CREDS_JSON", ""),
    )
    args = parser.parse_args()

    if not args.creds_json:
        raise RuntimeError("CREDS_JSON is empty")
    creds = json.loads(args.creds_json)

    cred_path = Path(args.credentials_file).expanduser()
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    cp = configparser.RawConfigParser()
    cp.read(cred_path)
    if not cp.has_section(args.profile):
        cp.add_section(args.profile)
    cp.set(args.profile, "aws_access_key_id", creds["AccessKeyId"])
    cp.set(args.profile, "aws_secret_access_key", creds["SecretAccessKey"])
    cp.set(args.profile, "aws_session_token", creds.get("SessionToken", ""))
    cp.set(args.profile, "expiration", creds.get("Expiration", ""))
    with cred_path.open("w", encoding="utf-8") as f:
        cp.write(f)

    workspace_path = Path(args.workspace_file).expanduser()
    with workspace_path.open("r", encoding="utf-8") as f:
        ws = json.load(f)

    settings = ws.setdefault("settings", {})
    connections = settings.setdefault("sqltools.connections", [])

    conn = None
    for item in connections:
        if (
            item.get("name") == args.connection_name
            and item.get("driver") == "driver.athena"
        ):
            conn = item
            break

    if conn is None:
        conn = {"name": args.connection_name, "driver": "driver.athena"}
        connections.append(conn)

    conn.update(
        {
            "connectionMethod": "Session Credentials",
            "accessKeyId": creds["AccessKeyId"],
            "secretAccessKey": creds["SecretAccessKey"],
            "sessionToken": creds.get("SessionToken", ""),
            "region": args.region,
            "workgroup": args.workgroup,
            "outputLocation": args.output_location,
            "previewLimit": conn.get("previewLimit", 200),
        }
    )
    conn.pop("profile", None)

    with workspace_path.open("w", encoding="utf-8") as f:
        json.dump(ws, f, ensure_ascii=False, indent=2)
        f.write("\n")

    conn_id = get_connection_id(conn)
    arg = urllib.parse.quote(json.dumps([conn_id]), safe="")
    uris = [
        f"vscode://command/sqltools.selectConnection?{arg}",
        f"cursor://command/sqltools.selectConnection?{arg}",
    ]
    launch_error = None
    for uri in uris:
        try:
            subprocess.run(["open", uri], check=True)
            launch_error = None
            print("SQLTools reconnect requested via URI:", uri)
            break
        except Exception as e:
            launch_error = e

    print("Synced credentials profile:", args.profile)
    print("Credentials file:", cred_path)
    print("SQLTools connection updated:", args.connection_name)
    print("Workspace file:", workspace_path)
    print("Expiration:", creds.get("Expiration", ""))
    if launch_error:
        print("Warning: could not trigger SQLTools reconnect automatically:", launch_error)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
