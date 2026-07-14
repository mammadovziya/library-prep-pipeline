from __future__ import annotations

import csv
import gzip
import io
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .limits import LIMITS


class InputLimitError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceRecord:
    index: int
    smiles: str
    source_id: str


class CountingReader(io.RawIOBase):
    def __init__(self, stream: io.BufferedReader, compressed_size: int):
        self.stream = stream
        self.compressed_size = compressed_size
        self.decompressed = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int:
        chunk = self.stream.read(len(buffer))
        if not chunk:
            return 0
        self.decompressed += len(chunk)
        ratio_limit = self.compressed_size * LIMITS.compression_ratio
        if self.decompressed > LIMITS.decompressed_bytes or self.decompressed > ratio_limit:
            raise InputLimitError("decompressed input or compression ratio limit exceeded")
        buffer[: len(chunk)] = chunk
        return len(chunk)


def _open_bounded(path: Path) -> io.BufferedReader:
    size = path.stat().st_size
    if size < 1 or size > LIMITS.compressed_bytes:
        raise InputLimitError("compressed input size is outside the allowed range")
    raw = path.open("rb")
    magic = raw.read(4)
    raw.seek(0)
    if magic.startswith(b"\x1f\x8b"):
        decoded: io.BufferedReader = io.BufferedReader(gzip.GzipFile(fileobj=raw, mode="rb"))
    elif magic.startswith((b"PK\x03\x04", b"BZh", b"\xfd7zX")):
        raw.close()
        raise InputLimitError("only plain text and a single gzip layer are accepted")
    else:
        decoded = io.BufferedReader(raw)
    counted = io.BufferedReader(CountingReader(decoded, size), buffer_size=64 * 1024)
    nested_magic = counted.peek(4)[:4]
    if nested_magic.startswith((b"\x1f\x8b", b"PK\x03\x04", b"BZh", b"\xfd7zX")):
        counted.close()
        raise InputLimitError("nested compression is prohibited")
    return counted


def iter_source_records(path: str | Path) -> Iterator[SourceRecord]:
    stream = _open_bounded(Path(path))
    try:
        index = 0
        smiles_column: int | None = None
        id_column: int | None = None
        header_seen = False
        while True:
            line = stream.readline(LIMITS.line_bytes + 1)
            if not line:
                break
            if len(line) > LIMITS.line_bytes:
                raise InputLimitError("line width exceeds 1 MiB")
            stripped = line.strip()
            if not stripped or stripped.startswith(b"#"):
                continue
            try:
                decoded = stripped.decode("utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise InputLimitError("input is not valid UTF-8") from exc
            try:
                if "\t" in decoded:
                    tokens = next(csv.reader([decoded], delimiter="\t", strict=True))
                elif "," in decoded:
                    tokens = next(csv.reader([decoded], delimiter=",", strict=True))
                else:
                    tokens = decoded.split()
            except csv.Error as exc:
                raise InputLimitError("CSV/TSV row is malformed or spans multiple lines") from exc
            if len(tokens) > LIMITS.columns:
                raise InputLimitError("input row exceeds 100 columns")
            if any(len(field.encode("utf-8")) > LIMITS.field_bytes for field in tokens):
                raise InputLimitError("input field exceeds 256 KiB")
            tokens = [token.strip() for token in tokens]
            if not tokens:
                continue

            if not header_seen:
                normalized = [token.casefold() for token in tokens]
                for candidate in ("smiles", "canonical_smiles", "smi", "smile"):
                    if candidate in normalized:
                        smiles_column = normalized.index(candidate)
                        break
                if smiles_column is not None:
                    for candidate in ("source_id", "id", "name", "molecule_id"):
                        if candidate in normalized:
                            id_column = normalized.index(candidate)
                            break
                    header_seen = True
                    continue
                header_seen = True

            selected_smiles = smiles_column if smiles_column is not None else 0
            if selected_smiles >= len(tokens):
                raise InputLimitError("input row is missing its SMILES column")
            smiles = tokens[selected_smiles]
            if smiles.upper() in {"SMILES", "CANONICAL_SMILES", "SMI", "SMILE"}:
                continue
            if "|" in smiles:
                smiles = smiles.split("|", 1)[0].rstrip()
            selected_id = id_column if id_column is not None else (1 if smiles_column in (None, 0) else None)
            source_id = tokens[selected_id] if selected_id is not None and selected_id < len(tokens) and tokens[selected_id] else f"record-{index}"
            yield SourceRecord(index=index, smiles=smiles, source_id=source_id)
            index += 1
    finally:
        stream.close()
