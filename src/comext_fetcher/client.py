from __future__ import annotations

import csv
import io
import logging
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from requests import Session

from .models import DownloadTarget

logger = logging.getLogger(__name__)

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/files"
LIST_PARAMS_DEFAULT: Dict[str, str] = {
    "format": "csv",
    "hierarchy": "false",  # flat listing; we supply full dir explicitly
    "sizeFormat": "NONE",  # raw bytes
    "dateFormat": "ISO",
}
HEADERS = {
    "User-Agent": "comext-products-downloader/1.0 (+https://example.org)",
}
DATA_GROUPS = {
    "products": "comext/COMEXT_DATA/PRODUCTS",
    "historical": "comext/COMEXT_HISTORICAL_DATA/PRODUCTS_1988_2001",
    "transport-hs": "comext/COMEXT_DATA/TRANSPORT_HS",
}
DEFAULT_DATA_GROUP = "products"
MONTH_RE = re.compile(
    r"(?:^|[/_])(?:full(?:_[a-z0-9]+)*|trhs_v2)[_]?(\d{6})\.7z$",
    re.IGNORECASE,
)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class EurostatClient:
    """HTTP client for querying the Eurostat bulk dissemination API."""

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        session: Optional[Session] = None,
        retries: int = 5,
        backoff: float = 1.5,
        timeout: float = 60.0,
    ) -> None:
        self.base_url = base_url
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout
        self._session = session or requests.Session()
        self._session.headers.update(HEADERS)

    @property
    def session(self) -> Session:
        return self._session

    def list_directory(self, dir_path: str) -> List[Dict[str, str]]:
        params = dict(LIST_PARAMS_DEFAULT)
        params["dir"] = dir_path
        response = self._get(self.base_url, params=params, stream=False)
        content = response.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        return [row for row in reader]

    def collect_targets(
        self,
        *,
        data_groups: Iterable[str],
        from_year: int,
        to_year: Optional[int] = None,
    ) -> List[DownloadTarget]:
        dirs = self._resolve_directories(data_groups)

        targets: List[DownloadTarget] = []
        for group, directory in dirs.items():
            rows = self.list_directory(directory)
            targets.extend(
                self._iter_monthly_targets(group, directory, rows, from_year, to_year)
            )

        # Deduplicate on (dir, name) just in case.
        unique: List[DownloadTarget] = []
        seen = set()
        for target in targets:
            key = (target.dir_path, target.name)
            if key in seen:
                continue
            seen.add(key)
            unique.append(target)

        unique.sort(key=lambda t: (t.yyyymm, t.name))
        return unique

    def download_target(
        self, target: DownloadTarget, dest_path: Path, chunk_size: int = 1_048_576
    ) -> None:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self.base_url}?file={target.dir_path}/{target.name}"
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".partial")

        with self._get(url, stream=True) as response, open(tmp_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    handle.write(chunk)

        if target.size is not None:
            actual_size = tmp_path.stat().st_size
            if actual_size != target.size:
                tmp_path.unlink(missing_ok=True)
                raise IOError(
                    f"Size mismatch for {target.name}: expected {target.size}, got {actual_size}"
                )

        tmp_path.replace(dest_path)

    def _resolve_directories(self, data_groups: Iterable[str]) -> Dict[str, str]:
        unique_groups: List[str] = []
        seen = set()
        for group in data_groups:
            if group in seen:
                continue
            if group not in DATA_GROUPS:
                raise ValueError(f"Unknown Eurostat data group: {group}")
            seen.add(group)
            unique_groups.append(group)

        if not unique_groups:
            raise ValueError("At least one Eurostat data group must be specified.")

        return {group: DATA_GROUPS[group] for group in unique_groups}

    def _iter_monthly_targets(
        self,
        group: str,
        dir_path: str,
        rows: Iterable[Dict[str, str]],
        from_year: int,
        to_year: Optional[int],
    ) -> Iterable[DownloadTarget]:
        for row in rows:
            if (row.get("TYPE") or "").lower() != "7z":
                continue

            name = row.get("NAME", "")
            if "partxixu" in name.lower():
                # Skip auxiliary partxixu variants.
                continue

            match = MONTH_RE.search(name)
            if not match:
                continue

            yyyymm = match.group(1)
            year = int(yyyymm[:4])
            month = int(yyyymm[4:])
            if not (1 <= month <= 12):
                # Skip annual/weekly aggregates such as 52 weeks.
                continue

            if year < from_year:
                continue
            if to_year is not None and year > to_year:
                continue

            size = self._parse_size(row.get("SIZE"))
            yield DownloadTarget(
                group=group, dir_path=dir_path, name=name, size=size, yyyymm=yyyymm
            )

    def _parse_size(self, value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _get(self, url: str, *, params=None, stream: bool) -> requests.Response:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    stream=stream,
                    timeout=self.timeout,
                )
                if response.status_code == 200:
                    return response
                if response.status_code in RETRYABLE_STATUS:
                    logger.debug(
                        "Retryable status %s on %s (attempt %s/%s)",
                        response.status_code,
                        url,
                        attempt,
                        self.retries,
                    )
                    time.sleep(self.backoff**attempt)
                    continue
                response.raise_for_status()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt == self.retries:
                    break
                logger.debug(
                    "Request error %s on %s (attempt %s/%s)",
                    exc,
                    url,
                    attempt,
                    self.retries,
                )
                time.sleep(self.backoff**attempt)
        if last_exc:
            raise last_exc
        raise RuntimeError("Failed to obtain response without raising an exception")
