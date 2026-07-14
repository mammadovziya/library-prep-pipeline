# Release matrix

Pin the final multi-architecture image digest, not only the tag. Values below are the intended qualification candidates as of 2026-07-14; digest, driver, kernel, and scientific results remain environment-specific evidence.

| Component | Candidate |
|---|---|
| Go | 1.26.5 exact toolchain |
| Next.js / React | 16.2.10 / 19.2.7 |
| PostgreSQL | 18.4, custom pgBackRest image |
| NATS Server | 2.14.0 |
| Caddy | 2.11.4 |
| Authentik | 2026.5.2 server + worker, no Docker socket |
| SeaweedFS | 4.29, Apache-2.0 |
| Prometheus / node exporter / blackbox exporter | 3.12.0 / 1.11.1 / 0.28.0 |
| AWS Go S3 transfer manager | 0.3.2; Seaweed multipart/conditional-write qualification required |
| Python | 3.11.13 |
| RDKit | 2025.03.5 |
| nvMolKit | 0.5.0 |
| Dimorphite-DL | 2.0.2 |
| PyArrow / pandas / NumPy | 21.0.0 / 2.3.2 / 2.2.6 |
| CUDA runtime candidate | 12.8.1 |
| GPUs | RTX 4090 (`sm_89`) and RTX 5090 (`sm_120`) |

Record for each promoted release:

- Host OS, exact kernel, NVIDIA driver, container toolkit.
- CUDA runtime, PyTorch CUDA build if pulled by nvMolKit, and native architecture targets.
- Every Python/conda package and channel artifact hash.
- Go module lock, pnpm lock, base image digests, final image digest, SBOM, and vulnerability scan.
- Chemistry image digest and sandbox policy hashes.

## Startup qualification

Every worker waits for an idle GPU, then runs the pinned chemistry image through sandboxd. The qualification uses real nvMolKit ETKDG and MMFF kernels on valid, charged, aromatic, and stereochemical molecules; checks conformer counts, nonzero coordinates, topology/stereochemistry, and output production. Failure prevents registration.

This smoke qualification does not replace the cross-generation release corpus.

## Scientific release corpus

Exact invariants:

- Input/stable record identity and canonical structure.
- Variant/conformer count, charge, stereochemistry, topology.
- Rejection classification and stable ordering.

Numerical tolerances:

- MMFF energy difference at most 0.1 kcal/mol.
- Symmetry-aware aligned heavy-atom RMSD at most 0.25 Å.
- Identical convergence/minimization classification.

Operational evidence records runtime, peak RAM/VRAM, scratch, output bytes, and throughput separately. If one image fails either GPU architecture, publish two immutable image variants behind the same task protocol.

The current nvMolKit production shard and final manifest record the actual stable range-derived ETKDG seed contract. The requested per-molecule derivation (`algorithm | molecule | variant | conformer`) must be demonstrated against the nvMolKit API and the cross-generation corpus before the reproducibility gate is signed; manifests must not name the target formula until it is the executed formula.
