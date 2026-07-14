from __future__ import annotations

import platform
import sys
from datetime import datetime, timezone
from importlib import metadata
from typing import Any


PACKAGE_NAMES = (
    "rdkit",
    "nvmolkit",
    "dimorphite-dl",
    "pyarrow",
    "numpy",
)


def _package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for name in PACKAGE_NAMES:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = "not-installed"
    try:
        versions["torch"] = metadata.version("torch")
    except metadata.PackageNotFoundError:
        pass
    return versions


def _cuda_backend() -> str | None:
    try:
        import torch

        return str(torch.version.cuda) if torch.version.cuda else None
    except (ImportError, AttributeError):
        return None


def build_provenance(spec: dict[str, Any]) -> dict[str, Any]:
    task_input = spec.get("input") if isinstance(spec.get("input"), dict) else {}
    attempt = spec.get("attempt") if isinstance(spec.get("attempt"), dict) else {}
    gpu_profile = spec.get("gpu_profile") if isinstance(spec.get("gpu_profile"), dict) else {}
    parent_artifacts: list[str] = []
    if spec.get("input_artifact_id"):
        parent_artifacts.append(str(spec["input_artifact_id"]))
    if isinstance(spec.get("artifacts"), list):
        parent_artifacts.extend(
            str(item["artifact_id"])
            for item in spec["artifacts"]
            if isinstance(item, dict) and item.get("artifact_id")
        )
    return {
        "manifest_created_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": str(spec.get("schema_version", "unknown")),
        "algorithm_version": str(spec.get("algorithm_version", "unknown")),
        "job_id": str(spec.get("job_id")) if spec.get("job_id") else None,
        "task_id": str(spec.get("task_id")) if spec.get("task_id") else None,
        "attempt_id": str(attempt.get("attempt_id")) if attempt.get("attempt_id") else None,
        "attempt_number": attempt.get("attempt_number"),
        "image_digest": spec.get("image_digest"),
        "gpu_uuid": spec.get("gpu_uuid"),
        "gpu_type": gpu_profile.get("type"),
        "driver_version": spec.get("driver_version"),
        "cuda_runtime": _cuda_backend(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "packages": _package_versions(),
        "input_checksum_sha256": task_input.get("checksum_sha256"),
        "parent_artifact_ids": sorted(set(parent_artifacts)),
        "parameters": {
            "preset": spec.get("preset"),
            "requested_conformers": spec.get("requested_conformers"),
            "range": spec.get("range"),
            "gpu_profile": gpu_profile,
        },
    }
