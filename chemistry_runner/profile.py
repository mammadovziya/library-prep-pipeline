from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .limits import LIMITS, bounded_rejection
from .provenance import build_provenance
from .safe_input import InputLimitError, iter_source_records


@dataclass
class Shard:
    index: int
    start_row: int
    end_row: int
    accepted_records: int
    estimated_cost: float


def _canonical_unique(molecules: list[Any]) -> list[Any]:
    from rdkit import Chem

    unique: dict[str, Any] = {}
    for molecule in molecules:
        if molecule is None:
            continue
        key = Chem.MolToSmiles(molecule, canonical=True, isomericSmiles=True)
        unique.setdefault(key, molecule)
    return [unique[key] for key in sorted(unique)]


def _protonate(smiles: str, minimum_ph: float, maximum_ph: float) -> list[str]:
    try:
        from dimorphite_dl.protonate import protonate_smiles

        try:
            result = protonate_smiles(smiles, min_ph=minimum_ph, max_ph=maximum_ph)
        except TypeError:
            result = protonate_smiles(smiles, min_ph=minimum_ph, max_ph=maximum_ph, precision=1.0)
        return [str(value) for value in result]
    except (ImportError, AttributeError):
        import dimorphite_dl
        from rdkit import Chem

        molecule = Chem.MolFromSmiles(smiles)
        if molecule is None:
            return []
        result = dimorphite_dl.run_with_mol_list([molecule], min_ph=minimum_ph, max_ph=maximum_ph)
        return [Chem.MolToSmiles(value, canonical=True, isomericSmiles=True) if hasattr(value, "GetAtoms") else str(value) for value in result]


def _chemistry_profile(smiles: str, conformers: int, enumerate_tautomers: bool, preset: str) -> dict[str, Any]:
    from rdkit import Chem
    from rdkit.Chem import Descriptors
    from rdkit.Chem.EnumerateStereoisomers import EnumerateStereoisomers, GetStereoisomerCount, StereoEnumerationOptions
    from rdkit.Chem.MolStandardize import rdMolStandardize

    molecule = Chem.MolFromSmiles(smiles)
    if molecule is None:
        return {"rejection": bounded_rejection("invalid_smiles")}
    atoms = molecule.GetNumAtoms()
    bonds = molecule.GetNumBonds()
    fragments = len(Chem.GetMolFrags(molecule))
    molecular_weight = float(Descriptors.MolWt(molecule))
    if atoms > LIMITS.atoms:
        return {"rejection": bounded_rejection("atom_limit")}
    if bonds > LIMITS.bonds:
        return {"rejection": bounded_rejection("bond_limit")}
    if fragments > LIMITS.fragments:
        return {"rejection": bounded_rejection("fragment_limit")}
    if molecular_weight > LIMITS.molecular_weight:
        return {"rejection": bounded_rejection("molecular_weight_limit")}

    parent = rdMolStandardize.FragmentParent(molecule)
    if parent is None or parent.GetNumAtoms() == 0:
        return {"rejection": bounded_rejection("fragment_parent_failed")}
    stereo_options = StereoEnumerationOptions(onlyUnassigned=True, maxIsomers=LIMITS.stereoisomers + 1)
    stereo_count = max(int(GetStereoisomerCount(parent, options=stereo_options)), 1)
    if stereo_count > LIMITS.stereoisomers:
        return {"rejection": bounded_rejection("stereoisomer_expansion_limit")}
    stereoisomers = _canonical_unique(list(EnumerateStereoisomers(parent, options=stereo_options)))
    if not stereoisomers:
        stereoisomers = [parent]
    if len(stereoisomers) > LIMITS.stereoisomers:
        return {"rejection": bounded_rejection("stereoisomer_expansion_limit")}
    tautomeric: list[Any] = []
    enumerator = rdMolStandardize.TautomerEnumerator()
    enumerator.SetMaxTautomers(LIMITS.tautomers + 1)
    for stereoisomer in stereoisomers:
        if enumerate_tautomers:
            candidates = list(enumerator.Enumerate(stereoisomer))
            if len(candidates) > LIMITS.tautomers:
                return {"rejection": bounded_rejection("tautomer_expansion_limit")}
            tautomeric.extend(candidates)
        else:
            tautomeric.append(enumerator.Canonicalize(stereoisomer))
    tautomeric = _canonical_unique(tautomeric)
    minimum_ph, maximum_ph = (6.4, 8.4) if preset == "enumerate" else (7.4, 7.4)
    protonated: list[Any] = []
    for tautomer in tautomeric:
        tautomer_smiles = Chem.MolToSmiles(tautomer, canonical=True, isomericSmiles=True)
        states = _protonate(tautomer_smiles, minimum_ph, maximum_ph)
        if not states:
            states = [tautomer_smiles]
        unique_states = sorted(set(states))
        if len(unique_states) > LIMITS.protonation_states:
            return {"rejection": bounded_rejection("protonation_expansion_limit")}
        protonated.extend(Chem.MolFromSmiles(state) for state in unique_states)
    prepared = _canonical_unique(protonated)
    if not prepared:
        return {"rejection": bounded_rejection("variant_preparation_failed")}
    if len(prepared) > LIMITS.total_variants:
        return {"rejection": bounded_rejection("total_variant_expansion_limit")}
    variants: list[dict[str, Any]] = []
    for variant in prepared:
        heavy_atoms = int(variant.GetNumHeavyAtoms())
        force_field_iteration_factor = 1.0 + min(2.0, float(Descriptors.NumRotatableBonds(variant)) / 10.0)
        variants.append({
            "canonical_smiles": Chem.MolToSmiles(variant, canonical=True, isomericSmiles=True),
            "heavy_atoms": heavy_atoms,
            "estimated_cost": heavy_atoms * conformers * force_field_iteration_factor,
        })
    return {
        "atoms": atoms,
        "bonds": bonds,
        "fragments": fragments,
        "molecular_weight": molecular_weight,
        "variants": variants,
        "rejection": None,
    }


def run_profile(spec: dict[str, Any], input_path: Path, output_dir: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    conformers = int(spec.get("requested_conformers", 1))
    preset = str(spec.get("preset", "docking"))
    if preset not in {"docking", "enumerate"}:
        raise InputLimitError("unsupported preparation preset")
    enumerate_tautomers = bool(spec.get("enumerate_tautomers", False))
    if conformers < 1 or conformers > LIMITS.conformers:
        raise InputLimitError("requested conformer count is outside the safety limit")
    target_cost = float(spec.get("target_shard_cost", 25_000_000))
    if not math.isfinite(target_cost) or target_cost <= 0:
        raise InputLimitError("target shard cost is invalid")

    schema = pa.schema([
        ("record_index", pa.int64()), ("source_record_index", pa.int64()), ("source_id", pa.string()), ("source_smiles", pa.string()),
        ("canonical_smiles", pa.string()), ("accepted", pa.bool_()), ("rejection", pa.string()),
        ("heavy_atoms", pa.int16()), ("predicted_variants", pa.int16()), ("estimated_cost", pa.float64()),
    ])
    profile_path = output_dir / "profile.parquet"
    writer = pq.ParquetWriter(profile_path, schema, compression="zstd", use_dictionary=True)
    batch: list[dict[str, Any]] = []
    shards: list[Shard] = []
    shard_start = 0
    shard_rows = 0
    shard_accepted = 0
    shard_cost = 0.0
    accepted = rejected = total = source_total = 0
    started = time.monotonic()

    def finish_shard(end_row: int) -> None:
        nonlocal shard_start, shard_rows, shard_accepted, shard_cost
        if shard_rows == 0:
            return
        shards.append(Shard(len(shards), shard_start, end_row, shard_accepted, round(shard_cost, 6)))
        shard_start = end_row
        shard_rows = shard_accepted = 0
        shard_cost = 0.0

    try:
        for record in iter_source_records(input_path):
            chemistry = _chemistry_profile(record.smiles, conformers, enumerate_tautomers, preset)
            rejection = chemistry.get("rejection")
            source_total += 1
            variants = chemistry.get("variants", [])
            if rejection is not None:
                variants = [{"canonical_smiles": None, "heavy_atoms": None, "estimated_cost": 0.0}]
                rejected += 1
            for variant_index, variant in enumerate(variants):
                cost = float(variant.get("estimated_cost", 0.0))
                if shard_rows and (shard_rows >= 50_000 or shard_cost + cost > target_cost):
                    finish_shard(total)
                row = {
                    "record_index": total,
                    "source_record_index": record.index,
                    "source_id": f"{record.source_id}#v{variant_index:02d}" if rejection is None else record.source_id,
                    "source_smiles": record.smiles,
                    "canonical_smiles": variant.get("canonical_smiles"),
                    "accepted": rejection is None,
                    "rejection": rejection,
                    "heavy_atoms": variant.get("heavy_atoms"),
                    "predicted_variants": len(variants) if rejection is None else 0,
                    "estimated_cost": cost,
                }
                batch.append(row)
                total += 1
                shard_rows += 1
                if rejection is None:
                    accepted += 1
                    shard_accepted += 1
                    shard_cost += cost
                if len(batch) >= 10_000:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema), row_group_size=10_000)
                    batch.clear()
        finish_shard(total)
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema), row_group_size=10_000)
    finally:
        writer.close()

    shard_path = output_dir / "shard_plan.json"
    shard_path.write_text(json.dumps({
        "schema_version": "1",
        "cost_formula": "heavy_atoms*predicted_variants*conformer_count*force_field_iteration_factor",
        "absolute_record_ceiling": 50_000,
        "target_cost": target_cost,
        "shards": [asdict(shard) for shard in shards],
    }, indent=2), encoding="utf-8")
    metrics = {
        "source_records": source_total, "prepared_records": total, "records_accepted": accepted, "source_records_rejected": rejected,
        "shards": len(shards), "wall_seconds": round(time.monotonic() - started, 3),
    }
    provenance = build_provenance(spec)
    (output_dir / "result.json").write_text(json.dumps({
        "artifacts": [
            {"path": "profile.parquet", "kind": "profile", "media_type": "application/vnd.apache.parquet", "manifest": {"records": total, "provenance": provenance}},
            {"path": "shard_plan.json", "kind": "shard_plan", "media_type": "application/json", "manifest": {
                "shard_count": len(shards),
                "shards": [asdict(shard) for shard in shards],
                "provenance": provenance,
            }},
        ],
        "metrics": metrics,
    }, indent=2), encoding="utf-8")
