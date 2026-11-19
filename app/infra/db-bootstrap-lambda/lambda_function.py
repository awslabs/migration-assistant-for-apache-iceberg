# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CloudFormation custom-resource Lambda that bootstraps IAM database auth.

Runs once on stack create/update. It:
  1. Reads the Aurora master credentials from Secrets Manager (never in code).
  2. Connects to the database as the master user (password auth, inside this
     Lambda only, over TLS).
  3. Creates a dedicated least-privilege application user (idempotent).
  4. Grants that user the `rds_iam` role so the app can authenticate with
     short-lived IAM tokens (no password) at runtime.
  5. Grants the app user the schema privileges it needs (CONNECT, USAGE,
     CREATE — the app creates and owns its own tables via init_db).
  6. Signals SUCCESS/FAILURE back to CloudFormation.

The application itself never uses a password and never connects as the master
user — it authenticates as APP_USER via IAM tokens.

Environment / resource properties (passed via the custom resource):
  DBSecretArn  — ARN of the Secrets Manager secret holding master creds
  DBHost       — Aurora cluster endpoint
  DBPort       — Aurora port (default 5432)
  DBName       — database name
  AppUser      — dedicated application user to create/grant (e.g. ima_app)
  Region       — AWS region
"""

import http.client
import json
import logging
import urllib.parse

import boto3
import pg8000.native

log = logging.getLogger()
log.setLevel(logging.INFO)

SUCCESS = "SUCCESS"
FAILED = "FAILED"


def _send_cfn_response(event, context, status, reason="", physical_id=None):
    """Signal a result back to CloudFormation via the pre-signed response URL."""
    response_url = event.get("ResponseURL")
    if not response_url:
        # Direct (non-CFN) invocation — nothing to signal
        log.info("No ResponseURL present; skipping CFN signal")
        return
    body = {
        "Status": status,
        "Reason": reason or f"See CloudWatch log stream: {context.log_stream_name}",
        "PhysicalResourceId": physical_id or context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": {},
    }
    data = json.dumps(body).encode("utf-8")
    # Send the response over an explicit HTTPS connection. The
    # CloudFormation-provided pre-signed ResponseURL is always HTTPS; we reject
    # any other scheme so a malformed/spoofed event cannot reach an unencrypted
    # or non-HTTP endpoint. Using http.client.HTTPSConnection (rather than
    # urllib.request.urlopen) keeps the transport HTTPS-only by construction —
    # the connection class cannot be redirected to file:/ or custom schemes.
    parsed = urllib.parse.urlparse(response_url)
    if parsed.scheme != "https":
        log.error("Refusing to send CFN response to non-HTTPS URL: %s", response_url[:50])
        return
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    try:
        conn = http.client.HTTPSConnection(parsed.netloc, timeout=30)
        try:
            conn.request(
                "PUT", path, body=data,
                headers={"Content-Type": "", "Content-Length": str(len(data))})
            resp = conn.getresponse()
            resp.read()  # drain so the connection can be reused/closed cleanly
            log.info("Sent %s response to CloudFormation (HTTP %s)", status, resp.status)
        finally:
            conn.close()
    except Exception as e:
        log.error("Failed to send CFN response: %s", e)


def _get_master_credentials(secret_arn: str, region: str) -> dict:
    """Fetch master DB credentials from Secrets Manager."""
    sm = boto3.client("secretsmanager", region_name=region)
    resp = sm.get_secret_value(SecretId=secret_arn)
    return json.loads(resp["SecretString"])


def _bootstrap(props: dict):
    """Create the app user and grant IAM auth + schema privileges. Idempotent."""
    region = props.get("Region", "us-east-1")
    secret_arn = props["DBSecretArn"]
    host = props["DBHost"]
    port = int(props.get("DBPort", 5432))
    dbname = props["DBName"]
    app_user = props["AppUser"]

    # SQL identifiers (database + app user names) cannot be parameterized, so
    # they are interpolated into DDL below. They come from the CloudFormation
    # stack template (not end-user input); validate them here as plain SQL
    # identifiers so a malformed value can never break out of the quoted
    # identifier context (defense-in-depth against SQL injection).
    if not dbname.isidentifier():
        raise ValueError(f"Invalid database name: {dbname!r}")
    if not app_user.isidentifier():
        raise ValueError(f"Invalid app user name: {app_user!r}")

    creds = _get_master_credentials(secret_arn, region)
    master_user = creds["username"]
    master_password = creds["password"]

    log.info("Connecting to %s:%s/%s as master user %s",
             host, port, dbname, master_user)
    conn = pg8000.native.Connection(
        user=master_user,
        password=master_password,
        host=host,
        port=port,
        database=dbname,
        ssl_context=True,  # require TLS
    )

    try:
        # Create the app user if it doesn't exist (idempotent).
        # A role with LOGIN and no password — it authenticates via IAM only.
        existing = conn.run(
            "SELECT 1 FROM pg_roles WHERE rolname = :u", u=app_user)
        if not existing:
            # Identifier already validated as a plain name at the top of
            # _bootstrap; safe to interpolate into the quoted identifier here.
            conn.run(f'CREATE USER "{app_user}" WITH LOGIN')
            log.info("Created app user %s", app_user)
        else:
            log.info("App user %s already exists", app_user)

        # Grant rds_iam so the user can authenticate with IAM tokens
        conn.run(f'GRANT rds_iam TO "{app_user}"')
        log.info("Granted rds_iam to %s", app_user)

        # Least-privilege schema grants — the app creates/owns its own tables
        conn.run(f'GRANT CONNECT ON DATABASE "{dbname}" TO "{app_user}"')
        conn.run(f'GRANT USAGE, CREATE ON SCHEMA public TO "{app_user}"')
        # Access to any pre-existing tables/sequences in public
        conn.run(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{app_user}"')
        conn.run(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{app_user}"')
        # Default privileges so future objects created by master are usable too
        conn.run(
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA public '
            f'GRANT ALL ON TABLES TO "{app_user}"')
        conn.run(
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA public '
            f'GRANT ALL ON SEQUENCES TO "{app_user}"')
        log.info("Granted schema privileges to %s", app_user)
    finally:
        conn.close()


def handler(event, context):
    log.info("Received event: %s", json.dumps({k: v for k, v in event.items()
                                                if k != "ResourceProperties"}))
    request_type = event.get("RequestType", "Create")

    # On Delete we intentionally do nothing — dropping the app user could
    # orphan owned objects and break rollback. Leave it in place.
    if request_type == "Delete":
        _send_cfn_response(event, context, SUCCESS, "Delete is a no-op")
        return

    try:
        props = event.get("ResourceProperties", {})
        _bootstrap(props)
        _send_cfn_response(event, context, SUCCESS,
                           "Database bootstrap complete")
    except Exception as e:
        log.exception("Bootstrap failed")
        _send_cfn_response(event, context, FAILED, str(e))
