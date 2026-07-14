from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .provenance import build_provenance


def run_qualification(spec: dict[str, Any], output_dir: Path) -> None:
    from rdkit import Chem
    from rdkit.Chem import AllChem, SDWriter
    from rdkit.Chem.rdDistGeom import ETKDGv3
    from nvmolkit.embedMolecules import EmbedMolecules
    from nvmolkit.mmffOptimization import MMFFOptimizeMoleculesConfs
    from nvmolkit.types import HardwareOptions

    profile = spec.get("gpu_profile", {})
    batch_size = max(1, min(int(profile.get("nvmolkit_batch", 8)), 8))
    hardware = HardwareOptions(preprocessingThreads=2, batchSize=batch_size, batchesPerGpu=1, gpuIds=[])
    corpus = [
        ("ethanol", "CCO"),
        ("stereo", "C[C@H](O)C(=O)O"),
        ("aromatic", "CC(=O)Oc1ccccc1C(=O)O"),
        ("charged", "C[NH2+]CC(=O)[O-]"),
    ]
    molecules = []
    canonical_before = []
    for name, smiles in corpus:
        molecule = Chem.MolFromSmiles(smiles)
        if molecule is None:
            raise RuntimeError("qualification corpus could not be parsed")
        canonical_before.append(Chem.MolToSmiles(molecule, canonical=True, isomericSmiles=True))
        molecule = Chem.AddHs(molecule)
        molecule.SetProp("_Name", name)
        molecules.append(molecule)

    parameters = ETKDGv3()
    parameters.useRandomCoords = True
    parameters.randomSeed = 0x5EED
    started = time.monotonic()
    EmbedMolecules(molecules, parameters, confsPerMolecule=2, hardwareOptions=hardware)
    if any(molecule.GetNumConformers() != 2 for molecule in molecules):
        raise RuntimeError("qualification did not produce the expected conformer count")
    for molecule in molecules:
        positions = molecule.GetConformer(0).GetPositions()
        if not positions.any():
            raise RuntimeError("qualification produced zero coordinates")
    optimizable = [molecule for molecule in molecules if AllChem.MMFFGetMoleculeProperties(molecule, mmffVariant="MMFF94s") is not None]
    energies = MMFFOptimizeMoleculesConfs(optimizable, maxIters=100, hardwareOptions=hardware)
    if len(energies) != len(optimizable):
        raise RuntimeError("qualification MMFF result count mismatch")
    canonical_after = [Chem.MolToSmiles(Chem.RemoveHs(molecule), canonical=True, isomericSmiles=True) for molecule in molecules]
    if canonical_before != canonical_after:
        raise RuntimeError("qualification changed topology or stereochemistry")

    sdf_path = output_dir / "qualification.sdf"
    writer = SDWriter(str(sdf_path))
    for molecule in molecules:
        for conformer_id in range(molecule.GetNumConformers()):
            writer.write(molecule, confId=conformer_id)
    writer.close()
    report = {
        "schema_version": "1",
        "molecules": len(molecules),
        "conformers": sum(molecule.GetNumConformers() for molecule in molecules),
        "mmff_optimized_molecules": len(optimizable),
        "canonical_invariants": True,
        "provenance": build_provenance(spec),
        "wall_seconds": round(time.monotonic() - started, 3),
    }
    (output_dir / "qualification.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (output_dir / "result.json").write_text(json.dumps({
        "artifacts": [{"path": "qualification.json", "kind": "qualification", "media_type": "application/json", "manifest": report}],
        "metrics": report,
    }, indent=2), encoding="utf-8")
