from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class DownloadTarget:
    """Metadata describing a single Eurostat archive to fetch."""

    group: str
    dir_path: str
    name: str
    size: Optional[int]
    yyyymm: str

    @property
    def filename(self) -> str:
        return Path(self.name).name

    @property
    def year(self) -> int:
        return int(self.yyyymm[:4])

    @property
    def month(self) -> int:
        return int(self.yyyymm[4:])
