from dataclasses import dataclass


GIB = 1024**3
MIB = 1024**2
KIB = 1024


@dataclass(frozen=True)
class InputLimits:
    compressed_bytes: int = 20 * GIB
    decompressed_bytes: int = 50 * GIB
    compression_ratio: int = 100
    line_bytes: int = 1 * MIB
    field_bytes: int = 256 * KIB
    columns: int = 100
    atoms: int = 256
    bonds: int = 512
    fragments: int = 16
    molecular_weight: float = 5_000.0
    stereoisomers: int = 4
    tautomers: int = 5
    protonation_states: int = 4
    conformers: int = 10
    total_variants: int = 40
    rejection_message_bytes: int = 1 * KIB


LIMITS = InputLimits()


def bounded_rejection(code: str, detail: str = "") -> str:
    message = code if not detail else f"{code}: {detail}"
    encoded = message.encode("utf-8", errors="replace")
    if len(encoded) <= LIMITS.rejection_message_bytes:
        return message
    return encoded[: LIMITS.rejection_message_bytes].decode("utf-8", errors="ignore")
