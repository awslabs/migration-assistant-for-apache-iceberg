# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CLI entry point — run as API server or execute commands."""

import argparse
import json
import os
import sys

from .core.engine import MigrationEngine
from .db import init_db, PipelineRepo


def main():
    parser = argparse.ArgumentParser(
        description="IMA engine — Iceberg table conversion",
        prog="ima-engine",
    )
    sub = parser.add_subparsers(dest="command")

    # serve — start the API server
    srv = sub.add_parser("serve", help="Start the API server")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8001)

    # init — initialize the database
    sub.add_parser("init", help="Initialize the database")

    # submit — submit a spec JSON file to create a pipeline
    sm = sub.add_parser("submit", help="Submit a spec JSON file")
    sm.add_argument("spec_file", help="Path to the spec JSON file")

    # validate — check if a spec is valid
    val = sub.add_parser("validate", help="Validate a spec file")
    val.add_argument("spec_file", help="Path to the spec JSON file")

    # status — check pipeline status
    st = sub.add_parser("status", help="Get pipeline status")
    st.add_argument("pipeline_id")

    # list — list pipelines
    ls = sub.add_parser("list", help="List pipelines")
    ls.add_argument("--status", default=None)

    # start/pause/resume/cancel
    for cmd in ("start", "pause", "resume", "cancel"):
        p = sub.add_parser(cmd, help=f"{cmd.title()} a pipeline")
        p.add_argument("pipeline_id")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "serve":
        import logging
        import uvicorn
        level = os.getenv("ENGINE_LOG_LEVEL", "INFO").upper()
        log_format = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"
        logging.basicConfig(
            level=getattr(logging, level, logging.INFO),
            format=log_format,
            datefmt=date_format,
        )
        log_file = os.getenv("ENGINE_LOG_FILE", "")
        if log_file:
            file_handler = logging.FileHandler(log_file, mode="a")
            file_handler.setLevel(getattr(logging, level, logging.INFO))
            file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
            logging.getLogger().addHandler(file_handler)
        init_db()
        uvicorn.run(
            "src.api.app:app",
            host=args.host,
            port=args.port,
            reload=False,
        )
        return

    if args.command == "init":
        init_db()
        return

    engine = MigrationEngine()
    repo = PipelineRepo()

    if args.command == "validate":
        from .core.spec import MigrationSpec
        try:
            spec = MigrationSpec.load(args.spec_file)
            spec_dict = spec.to_dict()
            temp = {
                "source_location": spec_dict["source"]["location"],
                "source_format": spec_dict["source"]["format"],
                "source_partitioned": spec_dict["source"].get("partitioned", False),
                "target_type": spec_dict["target"]["type"],
                "migration_type": spec_dict["strategy"]["type"],
            }
            ok, reason = engine.validate(temp)
            print(json.dumps({"valid": ok, "reason": reason}))
            sys.exit(0 if ok else 1)
        except Exception as e:
            print(json.dumps({"error": str(e)}))
            sys.exit(1)

    elif args.command == "submit":
        from .core.spec import MigrationSpec
        init_db()
        try:
            spec = MigrationSpec.load(args.spec_file)
            spec_dict = spec.to_dict()
            temp = {
                "source_location": spec_dict["source"]["location"],
                "source_format": spec_dict["source"]["format"],
                "source_partitioned": spec_dict["source"].get("partitioned", False),
                "target_type": spec_dict["target"]["type"],
                "migration_type": spec_dict["strategy"]["type"],
            }
            ok, reason = engine.validate(temp)
            if not ok:
                print(json.dumps({"error": f"Validation failed: {reason}"}))
                sys.exit(1)
            p = repo.create_from_spec(spec_dict)
            print(json.dumps({
                "pipeline_id": p["id"],
                "source": f"{p['source_database']}.{p['source_table']}",
                "target": f"{p['target_type']}:{p['target_database']}.{p['target_table']}",
                "migration_type": p["migration_type"],
                "status": p["status"],
            }, indent=2))
        except ValueError as e:
            print(json.dumps({"error": str(e)}))
            sys.exit(1)
        except Exception as e:
            print(json.dumps({"error": f"Failed to submit spec: {str(e)}"}))
            sys.exit(1)

    elif args.command == "status":
        init_db()
        p = repo.get(args.pipeline_id)
        if not p:
            print("Pipeline not found")
            sys.exit(1)
        print(json.dumps(p, indent=2, default=str))

    elif args.command == "list":
        init_db()
        rows = repo.list_all(status=args.status)
        print(json.dumps(rows, indent=2, default=str))

    elif args.command in ("start", "pause", "resume", "cancel"):
        init_db()
        p = repo.get(args.pipeline_id)
        if not p:
            print("Pipeline not found")
            sys.exit(1)

        if args.command == "start":
            if p["status"] not in ("pending", "paused"):
                print(json.dumps({"error": f"Cannot start pipeline in '{p['status']}' status"}))
                sys.exit(1)
            result = engine.submit(p)
        elif args.command == "cancel":
            if p["status"] in ("completed", "cancelled"):
                print(json.dumps({"error": f"Cannot cancel '{p['status']}' pipeline"}))
                sys.exit(1)
            result = engine.cancel(p)
        elif args.command == "resume":
            if p["status"] != "paused":
                print(json.dumps({"error": "Can only resume paused pipelines"}))
                sys.exit(1)
            result = engine.submit(p)
        elif args.command == "pause":
            if p["status"] != "running":
                print(json.dumps({"error": "Can only pause running pipelines"}))
                sys.exit(1)
            result = repo.update_status(args.pipeline_id, "paused")

        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
