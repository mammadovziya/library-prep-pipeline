import gzip
from pathlib import Path

import pytest

from chemistry_runner.limits import LIMITS, bounded_rejection
from chemistry_runner.safe_input import InputLimitError, iter_source_records


def test_plain_and_gzip_inputs(tmp_path: Path) -> None:
    plain = tmp_path / "source.smi"
    plain.write_text("SMILES ID\nCCO ethanol\nCCN\tamine\n", encoding="utf-8")
    assert [row.source_id for row in iter_source_records(plain)] == ["ethanol", "amine"]

    compressed = tmp_path / "source.gz"
    with gzip.open(compressed, "wb") as stream:
        stream.write(b"CCO compound-1\n")
    assert next(iter_source_records(compressed)).smiles == "CCO"


def test_csv_and_tsv_headers_select_server_supported_columns(tmp_path: Path) -> None:
    csv_source = tmp_path / "source.csv"
    csv_source.write_text('molecule_id,smiles,note\ncompound-1,"C[C@H](O)C",alpha\n', encoding="utf-8")
    csv_row = next(iter_source_records(csv_source))
    assert (csv_row.smiles, csv_row.source_id) == ("C[C@H](O)C", "compound-1")

    tsv_source = tmp_path / "source.tsv"
    tsv_source.write_text("name\tcanonical_smiles\textra\ncompound-2\tCCN\tbeta\n", encoding="utf-8")
    tsv_row = next(iter_source_records(tsv_source))
    assert (tsv_row.smiles, tsv_row.source_id) == ("CCN", "compound-2")


def test_nested_compression_is_rejected(tmp_path: Path) -> None:
    nested = tmp_path / "nested.gz"
    with gzip.open(nested, "wb") as stream:
        stream.write(gzip.compress(b"CCO id\n"))
    with pytest.raises(InputLimitError, match="nested"):
        list(iter_source_records(nested))


def test_line_limit_is_enforced(tmp_path: Path) -> None:
    source = tmp_path / "large.smi"
    source.write_bytes(b"C" * (LIMITS.line_bytes + 1) + b" id\n")
    with pytest.raises(InputLimitError, match="line width"):
        list(iter_source_records(source))


def test_rejection_message_is_bounded() -> None:
    result = bounded_rejection("invalid", "x" * 10_000)
    assert len(result.encode()) <= LIMITS.rejection_message_bytes
