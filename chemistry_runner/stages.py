from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Iterator

from .limits import bounded_rejection
from .provenance import build_provenance


def _profile_rows(path: Path, start: int, end: int) -> Iterator[dict[str, Any]]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    offset = 0
    columns = ["record_index", "source_id", "canonical_smiles", "accepted"]
    for batch in parquet.iter_batches(batch_size=10_000, columns=columns):
        batch_end = offset + batch.num_rows
        if batch_end <= start:
            offset = batch_end
            continue
        if offset >= end:
            break
        local_start = max(0, start - offset)
        local_end = min(batch.num_rows, end - offset)
        selected = batch.slice(local_start, local_end - local_start).to_pylist()
        yield from selected
        offset = batch_end


def _stable_seed(algorithm: str, first_record: int, last_record: int) -> int:
    digest = hashlib.sha256(f"{algorithm}|{first_record}|{last_record}".encode()).digest()
    return int.from_bytes(digest[:4], "big") & 0x7FFFFFFF


def run_conformer(spec: dict[str, Any], input_path: Path, output_dir: Path) -> None:
    from rdkit import Chem
    from rdkit.Chem import AllChem, SDWriter
    from rdkit.Chem.rdDistGeom import ETKDGv3
    from nvmolkit.embedMolecules import EmbedMolecules
    from nvmolkit.mmffOptimization import MMFFOptimizeMoleculesConfs
    from nvmolkit.types import HardwareOptions

    row_range = spec.get("range", {})
    start, end = int(row_range.get("start_row", -1)), int(row_range.get("end_row", -1))
    if start < 0 or end <= start or end - start > 50_000:
        raise ValueError("invalid conformer shard range")
    conformers = int(spec.get("requested_conformers", 1))
    if conformers < 1 or conformers > 10:
        raise ValueError("invalid conformer count")
    profile = spec.get("gpu_profile", {})
    fallback = bool(spec.get("attempt", {}).get("use_oom_fallback", False))
    engine_chunk = int(profile.get("oom_fallback_chunk" if fallback else "engine_chunk", 25_000))
    batch_size = int(profile.get("oom_fallback_batch" if fallback else "nvmolkit_batch", 128))
    batches_per_gpu = int(profile.get("batches_per_gpu", 1))
    engine_chunk = max(1, min(engine_chunk, 50_000))
    algorithm = str(spec.get("algorithm_version", "unknown"))
    seed = _stable_seed(algorithm, start, end)

    params = ETKDGv3()
    params.useRandomCoords = True
    params.randomSeed = seed
    hardware = HardwareOptions(
        preprocessingThreads=8,
        batchSize=max(1, batch_size),
        batchesPerGpu=max(1, batches_per_gpu),
        gpuIds=[],
    )
    sdf_path = output_dir / f"shard-{start:09d}-{end:09d}.sdf"
    rejection_path = output_dir / f"shard-{start:09d}-{end:09d}-rejections.jsonl"
    writer = SDWriter(str(sdf_path))
    rejection_file = rejection_path.open("w", encoding="utf-8")
    accepted: list[tuple[int, str, Any]] = []
    written = rejected = 0
    started = time.monotonic()

    def flush() -> None:
        nonlocal accepted, written, rejected
        if not accepted:
            return
        molecules = [item[2] for item in accepted]
        try:
            EmbedMolecules(molecules, params, confsPerMolecule=conformers, hardwareOptions=hardware)
            optimizable: list[Any] = []
            without_mmff: list[Any] = []
            for molecule in molecules:
                if AllChem.MMFFGetMoleculeProperties(molecule, mmffVariant="MMFF94s") is None:
                    without_mmff.append(molecule)
                else:
                    optimizable.append(molecule)
            energies = MMFFOptimizeMoleculesConfs(optimizable, maxIters=200, hardwareOptions=hardware)
            energy_by_molecule = {id(molecule): molecule_energies for molecule, molecule_energies in zip(optimizable, energies)}
            without_mmff_ids = {id(molecule) for molecule in without_mmff}
            for molecule in molecules:
                molecule_energies = energy_by_molecule.get(id(molecule), [])
                for conformer_id in range(molecule.GetNumConformers()):
                    if conformer_id < len(molecule_energies):
                        molecule.SetProp("MMFF_Energy", f"{molecule_energies[conformer_id]:.6f}")
                    molecule.SetProp("MMFF_Minimised", "False" if id(molecule) in without_mmff_ids else "True")
                    writer.write(molecule, confId=conformer_id)
                    written += 1
        except RuntimeError as exc:
            if "out of memory" in str(exc).lower() or "cuda" in str(exc).lower() and "memory" in str(exc).lower():
                raise MemoryError("CUDA memory exhausted") from exc
            raise
        finally:
            accepted = []

    try:
        for row in _profile_rows(input_path, start, end):
            if not row["accepted"]:
                continue
            molecule = Chem.MolFromSmiles(row["canonical_smiles"])
            if molecule is None:
                rejection_file.write(json.dumps({
                    "record_index": row["record_index"],
                    "code": bounded_rejection("profiled_structure_reparse_failed"),
                }) + "\n")
                rejected += 1
                continue
            molecule = Chem.AddHs(molecule)
            molecule.SetProp("_Name", str(row["source_id"]))
            molecule.SetProp("LibraryPrep_RecordIndex", str(row["record_index"]))
            molecule.SetProp("LibraryPrep_Seed", str(seed))
            accepted.append((row["record_index"], row["source_id"], molecule))
            if len(accepted) >= engine_chunk:
                flush()
        flush()
    finally:
        writer.close()
        rejection_file.close()

    manifest = {
        "schema_version": "1",
        "algorithm_version": algorithm,
        "range": {"start_row": start, "end_row": end},
        "conformers_written": written,
        "rejected_records": rejected,
        "seed": seed,
        "seed_scope": "stable shard range",
        "gpu_profile": profile,
        "image_digest": spec.get("image_digest"),
        "gpu_uuid": spec.get("gpu_uuid"),
        "attempt": spec.get("attempt"),
        "provenance": build_provenance(spec),
        "wall_seconds": round(time.monotonic() - started, 3),
    }
    artifacts = [{
        "path": sdf_path.name,
        "kind": "conformer_shard",
        "media_type": "chemical/x-mdl-sdfile",
        "manifest": manifest,
    }]
    if rejection_path.stat().st_size:
        artifacts.append({
            "path": rejection_path.name,
            "kind": "rejections",
            "media_type": "application/x-ndjson",
            "manifest": {"records": rejected, "range": manifest["range"]},
        })
    else:
        rejection_path.unlink()
    (output_dir / "result.json").write_text(json.dumps({
        "artifacts": artifacts,
        "metrics": manifest,
    }, indent=2), encoding="utf-8")


def run_finalize(spec: dict[str, Any], output_dir: Path) -> None:
    artifacts = spec.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("finalizer artifact list is invalid")
    manifest = {
        "schema_version": "1",
        "job_id": spec.get("job_id"),
        "algorithm_version": spec.get("algorithm_version"),
        "preset": spec.get("preset"),
        "requested_conformers": spec.get("requested_conformers"),
        "seed_derivation": spec.get("seed_derivation"),
        "artifacts": artifacts,
        "quarantined_tasks": spec.get("quarantined_tasks", []),
        "created_at_unix": int(time.time()),
        "provenance": build_provenance(spec),
    }
    path = output_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "result.json").write_text(json.dumps({
        "artifacts": [{"path": path.name, "kind": "final_manifest", "media_type": "application/json", "manifest": {"artifact_count": len(artifacts), "provenance": manifest["provenance"]} }],
        "metrics": {"artifact_count": len(artifacts)},
    }, indent=2), encoding="utf-8")
