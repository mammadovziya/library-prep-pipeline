from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .profile import run_profile
from .qualification import run_qualification
from .safe_input import InputLimitError
from .stages import run_conformer, run_finalize


def main() -> int:
    parser = argparse.ArgumentParser(prog="chemistry-runner")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--spec", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--scratch", required=True)
    args = parser.parse_args()
    spec_path = Path(args.spec)
    output_dir, scratch_dir = Path(args.output), Path(args.scratch)
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if spec.get("schema_version") != "1":
            raise InputLimitError("unsupported task schema")
        output_dir.mkdir(parents=True, exist_ok=True)
        scratch_dir.mkdir(parents=True, exist_ok=True)
        if spec.get("stage") == "profile":
            local_input = Path(spec.get("local_input", "/work/input/source"))
            run_profile(spec, local_input, output_dir)
        elif spec.get("stage") == "conformer":
            local_input = Path(spec.get("local_input", "/work/input/source"))
            run_conformer(spec, local_input, output_dir)
        elif spec.get("stage") == "finalize":
            run_finalize(spec, output_dir)
        elif spec.get("stage") == "qualification":
            run_qualification(spec, output_dir)
        else:
            raise InputLimitError("unsupported task stage")
        return 0
    except InputLimitError as exc:
        print(f"task rejected: {type(exc).__name__}", file=sys.stderr)
        return 20
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        print("task specification is invalid", file=sys.stderr)
        return 42
    except MemoryError:
        print("runner memory allocation failed", file=sys.stderr)
        return 70
    except Exception as exc:  # container boundary classifies this as infrastructure
        print(f"runner failed: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
