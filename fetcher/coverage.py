from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Set, Tuple

from .client import MONTH_RE
from .models import DownloadTarget


@dataclass
class CoverageError(RuntimeError):
    message: str

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


def build_expected_months(targets: Iterable[DownloadTarget]) -> Dict[int, Set[int]]:
    expected: DefaultDict[int, Set[int]] = defaultdict(set)
    for target in targets:
        expected[target.year].add(target.month)
    return dict(expected)


def summarize_local_months(dest_root: Path) -> Dict[int, Set[int]]:
    local: DefaultDict[int, Set[int]] = defaultdict(set)
    for path in dest_root.glob("*.7z"):
        match = MONTH_RE.search(path.name)
        if not match:
            continue
        yyyymm = match.group(1)
        year = int(yyyymm[:4])
        month = int(yyyymm[4:])
        if 1 <= month <= 12:
            local[year].add(month)
    return dict(local)


def assert_monthly_coverage(dest_root: Path, expected: Dict[int, Set[int]]) -> None:
    current_year = date.today().year
    local = summarize_local_months(dest_root)
    missing: List[Tuple[int, Set[int], Set[int]]] = []
    missing_current: List[Tuple[int, Set[int]]] = []

    for year in sorted(expected):
        expected_months = expected[year]
        local_months = local.get(year, set())
        if year < current_year:
            required = set(range(1, 13))
            remote_gap = required - expected_months
            absent = required - local_months
            if absent:
                missing.append((year, absent, remote_gap))
        elif year == current_year:
            absent = expected_months - local_months
            if absent:
                missing_current.append((year, absent))

    if missing or missing_current:
        lines = ["⚠️ Missing monthly files detected:"]
        for year, months, remote_gap in missing:
            month_list = ", ".join(f"{m:02d}" for m in sorted(months))
            line = f"  {year}: missing months {month_list}"
            if remote_gap:
                remote_list = ", ".join(f"{m:02d}" for m in sorted(remote_gap))
                line += f" (API listing also missing {remote_list})"
            lines.append(line)
        for year, months in missing_current:
            month_list = ", ".join(f"{m:02d}" for m in sorted(months))
            lines.append(
                f"  {year}: missing months {month_list} compared to current API listing"
            )
        raise CoverageError("\n".join(lines))
