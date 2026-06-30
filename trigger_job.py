#!/usr/bin/env python3
"""
Start the data-validator Cloud Run Job using a clean JSON payload file.

Example:
  python trigger_job.py payload.json
  python trigger_job.py --dry-run payload.json   # print request, do not execute
"""

from __future__ import annotations

import argparse
import json
import sys

from shared import utils
from shared.payload_loader import load_payload_dict
from shared.run_job_services import build_run_job_request_body, start_validation_job

utils.setup_project_environment()


def main() -> int:
    parser = argparse.ArgumentParser(description="Trigger data-validator Cloud Run Job")
    parser.add_argument(
        "payload_file",
        nargs="?",
        help="Path to JSON payload (same shape as the HTTP API body)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the Run Job API body without executing",
    )
    args = parser.parse_args()

    if args.payload_file:
        from shared.payload_loader import _load_json_file

        payload = _load_json_file(args.payload_file)
    else:
        try:
            payload = load_payload_dict()
        except RuntimeError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1

    if args.dry_run:
        body = json.loads(build_run_job_request_body(payload=payload).decode())
        print(json.dumps({"payload": payload, "run_job_api_body": body}, indent=2))
        return 0

    try:
        result = start_validation_job(payload=payload)
    except Exception as e:
        print(f"error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    print(json.dumps({"status": "accepted", **result}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
