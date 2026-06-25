#!/usr/bin/env python3
"""Clone a Tableau datasource, replace Custom SQL, publish as new datasource."""

from __future__ import annotations

import os
import json
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Optional
from urllib import error as urlerror
from urllib import request as urlrequest
from xml.etree import ElementTree as ET

import sys


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


DEFAULT_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.27")
DEFAULT_SQL_FILE = "cc_reports/duplicates.sql"
DEFAULT_SQL_CODE = "duplicates"


class SimpleHTTPError(Exception):
    def __init__(self, status_code: int, body: str):
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code
        self.body = body


def http_request(
    method: str,
    url: str,
    headers: Optional[dict[str, str]] = None,
    body: Optional[bytes] = None,
) -> tuple[int, bytes]:
    req = urlrequest.Request(url=url, data=body, headers=headers or {}, method=method)
    try:
        with urlrequest.urlopen(req, timeout=300) as resp:
            return resp.status, resp.read()
    except urlerror.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        raise SimpleHTTPError(exc.code, err_body) from exc


def sign_in(server_url: str, api_version: str, site_content_url: str, pat_name: str, pat_secret: str) -> tuple[str, str]:
    url = f"{server_url}/api/{api_version}/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": site_content_url},
        }
    }
    _, raw = http_request(
        method="POST",
        url=url,
        headers={"Content-Type": "application/json"},
        body=json.dumps(payload).encode("utf-8"),
    )
    data = json.loads(raw.decode("utf-8"))
    token = data["credentials"]["token"]
    site_id = data["credentials"]["site"]["id"]
    return token, site_id


def sign_out(server_url: str, api_version: str, token: str) -> None:
    url = f"{server_url}/api/{api_version}/auth/signout"
    try:
        http_request(method="POST", url=url, headers={"X-Tableau-Auth": token})
    except SimpleHTTPError:
        pass


def resolve_sql_file(sql_input: str, repo_root: Path) -> Path:
    raw = sql_input.strip()
    if not raw:
        return repo_root / DEFAULT_SQL_FILE

    as_path = Path(raw).expanduser()
    if as_path.is_absolute() and as_path.exists():
        return as_path

    rel = repo_root / raw
    if rel.exists():
        return rel

    if "/" not in raw and "\\" not in raw:
        code_candidate = repo_root / "cc_reports" / f"{raw}.sql"
        if code_candidate.exists():
            return code_candidate

    raise FileNotFoundError(f"SQL file not found for input '{sql_input}'")


def download_template_datasource(
    server_url: str,
    api_version: str,
    site_id: str,
    token: str,
    datasource_id: str,
    output_path: Path,
) -> None:
    url = f"{server_url}/api/{api_version}/sites/{site_id}/datasources/{datasource_id}/content"
    _, raw = http_request(method="GET", url=url, headers={"X-Tableau-Auth": token})
    output_path.write_bytes(raw)


def replace_custom_sql_in_tds(tds_xml: bytes, new_sql: str) -> bytes:
    root = ET.fromstring(tds_xml)
    changed = 0

    for rel in root.findall(".//relation[@type='text']"):
        if "text" in rel.attrib:
            rel.attrib["text"] = new_sql
            changed += 1
        elif rel.text is not None:
            rel.text = new_sql
            changed += 1

    if changed == 0:
        raise RuntimeError("Could not find any <relation type='text'> to replace.")

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def mutate_datasource_custom_sql(template_path: Path, new_sql: str, output_path: Path) -> str:
    suffix = template_path.suffix.lower()
    if suffix == ".tds":
        updated = replace_custom_sql_in_tds(template_path.read_bytes(), new_sql)
        output_path.write_bytes(updated)
        return "tds"

    if suffix != ".tdsx":
        raise RuntimeError(f"Unsupported template extension: {template_path.name}")

    with zipfile.ZipFile(template_path, "r") as zin:
        names = zin.namelist()
        tds_candidates = [n for n in names if n.lower().endswith(".tds")]
        if not tds_candidates:
            raise RuntimeError("No .tds found inside .tdsx package")
        tds_inner_name = tds_candidates[0]
        tds_bytes = zin.read(tds_inner_name)
        updated_tds = replace_custom_sql_in_tds(tds_bytes, new_sql)

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zout:
            for name in names:
                if name == tds_inner_name:
                    zout.writestr(name, updated_tds)
                else:
                    zout.writestr(name, zin.read(name))
    return "tdsx"


def publish_datasource(
    server_url: str,
    api_version: str,
    site_id: str,
    token: str,
    project_id: str,
    datasource_name: str,
    datasource_file_type: str,
    datasource_file_path: Path,
) -> dict:
    url = (
        f"{server_url}/api/{api_version}/sites/{site_id}/datasources"
        f"?overwrite=false"
    )
    boundary = f"----------{uuid.uuid4().hex}"

    xml_payload = (
        "<tsRequest>"
        f"<datasource name=\"{datasource_name}\">"
        f"<project id=\"{project_id}\" />"
        "</datasource>"
        "</tsRequest>"
    ).encode("utf-8")
    file_name = datasource_file_path.name
    file_bytes = datasource_file_path.read_bytes()

    body = b"".join(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            b'Content-Disposition: name="request_payload"\r\n',
            b"Content-Type: text/xml\r\n\r\n",
            xml_payload,
            b"\r\n",
            f"--{boundary}\r\n".encode("utf-8"),
            f'Content-Disposition: name="tableau_datasource"; filename="{file_name}"\r\n'.encode("utf-8"),
            b"Content-Type: application/octet-stream\r\n\r\n",
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )

    headers = {
        "X-Tableau-Auth": token,
        "Content-Type": f"multipart/mixed; boundary={boundary}",
    }
    _, raw = http_request(method="POST", url=url, headers=headers, body=body)
    try:
        return json.loads(raw.decode("utf-8"))
    except ValueError:
        return {"raw": raw.decode("utf-8", errors="replace"), "datasource_file_type": datasource_file_type}


def prompt_with_default(label: str, default: str) -> str:
    value = input(f"{label} [{default}]: ").strip()
    return value or default


def main() -> int:
    required = [
        "TABLEAU_SERVER_URL",
        "TABLEAU_SITE_CONTENT_URL",
        "TABLEAU_PAT_NAME",
        "TABLEAU_PAT_SECRET",
        "TABLEAU_TEMPLATE_DATASOURCE_ID",
        "TABLEAU_PROJECT_ID",
    ]

    try:
        for key in required:
            require_env(key)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    server_url = require_env("TABLEAU_SERVER_URL").rstrip("/")
    site_content_url = require_env("TABLEAU_SITE_CONTENT_URL")
    pat_name = require_env("TABLEAU_PAT_NAME")
    pat_secret = require_env("TABLEAU_PAT_SECRET")
    template_id = require_env("TABLEAU_TEMPLATE_DATASOURCE_ID")
    project_id = require_env("TABLEAU_PROJECT_ID")
    default_name = os.getenv("TABLEAU_NEW_DATASOURCE_NAME", "cc-tech-duplicates")
    repo_root = Path(__file__).resolve().parents[2]

    sql_input = prompt_with_default(
        "SQL code/path (e.g. duplicates or cc_reports/duplicates.sql)",
        DEFAULT_SQL_CODE,
    )
    new_datasource_name = prompt_with_default("New datasource name", default_name)

    try:
        sql_path = resolve_sql_file(sql_input, repo_root)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    new_sql = sql_path.read_text(encoding="utf-8")
    api_version = DEFAULT_API_VERSION

    token: Optional[str] = None
    try:
        token, site_id = sign_in(server_url, api_version, site_content_url, pat_name, pat_secret)

        with tempfile.TemporaryDirectory(prefix="tableau_ds_") as td:
            workdir = Path(td)
            downloaded = workdir / "template_datasource.tdsx"
            download_template_datasource(
                server_url=server_url,
                api_version=api_version,
                site_id=site_id,
                token=token,
                datasource_id=template_id,
                output_path=downloaded,
            )

            # If template was plain .tds, keep extension in output.
            if downloaded.read_bytes()[:2] == b"PK":
                template_path = downloaded
                output_ext = ".tdsx"
            else:
                template_path = workdir / "template_datasource.tds"
                template_path.write_bytes(downloaded.read_bytes())
                output_ext = ".tds"

            mutated = workdir / f"{new_datasource_name}{output_ext}"
            file_type = mutate_datasource_custom_sql(template_path, new_sql, mutated)
            result = publish_datasource(
                server_url=server_url,
                api_version=api_version,
                site_id=site_id,
                token=token,
                project_id=project_id,
                datasource_name=new_datasource_name,
                datasource_file_type=file_type,
                datasource_file_path=mutated,
            )
    except SimpleHTTPError as exc:
        print(f"HTTP error: {exc.status_code}", file=sys.stderr)
        if exc.body:
            print(exc.body, file=sys.stderr)
        return 1
    finally:
        if token:
            sign_out(server_url, api_version, token)

    print("Datasource published successfully.")
    print(f"SQL file used: {sql_path}")
    print(f"Datasource name: {new_datasource_name}")
    if isinstance(result, dict):
        ds_name = (
            result.get("datasource", {}).get("name")
            if isinstance(result.get("datasource"), dict)
            else None
        )
        if ds_name:
            print(f"Published datasource: {ds_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
