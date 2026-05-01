from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from .client import EurostatClient
from .models import DownloadTarget

logger = logging.getLogger(__name__)


@dataclass
class DownloadStats:
    downloaded: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)
    already_present: List[Path] = field(default_factory=list)

    def merge_error(self, message: str) -> None:
        self.errors.append(message)


def partition_existing_targets(
    targets: Sequence[DownloadTarget], dest_root: Path
) -> Tuple[List[DownloadTarget], List[Path]]:
    to_download: List[DownloadTarget] = []
    already_local: List[Path] = []
    for target in targets:
        candidate = dest_root / target.filename
        if (
            candidate.exists()
            and target.size is not None
            and candidate.stat().st_size == target.size
        ):
            already_local.append(candidate)
            continue
        to_download.append(target)
    return to_download, already_local


def download_all(
    client: EurostatClient,
    targets: Sequence[DownloadTarget],
    dest_root: Path,
    *,
    max_workers: int,
    logger_: logging.Logger,
    show_progress: bool = False,
    progress_desc: Optional[str] = None,
    log_items: bool = True,
) -> DownloadStats:
    stats = DownloadStats()
    to_download, already_local = partition_existing_targets(targets, dest_root)
    stats.skipped += len(already_local)
    stats.already_present.extend(already_local)

    if log_items:
        for path in already_local:
            logger_.info("Skipped (already exists): %s", path)

    if not to_download:
        return stats

    progress = None
    if show_progress:
        try:
            from tqdm.auto import tqdm  # type: ignore[import-not-found]
        except ImportError:  # pragma: no cover - optional dependency
            tqdm = None
        if tqdm is not None:
            progress = tqdm(
                total=len(to_download),
                desc=progress_desc or "Downloading",
                unit="file",
                leave=False,
            )

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_download_one, client, dest_root, target): target
                for target in to_download
            }
            for future in as_completed(future_map):
                target = future_map[future]
                try:
                    local_path = future.result()
                except Exception as exc:  # noqa: BLE001
                    message = f"{target.name}: {exc}"
                    stats.merge_error(message)
                    if log_items:
                        logger_.error("Error: %s", message)
                else:
                    stats.downloaded += 1
                    if log_items:
                        logger_.info("Downloaded successfully: %s", local_path)
                if progress is not None:
                    progress.update(1)
    finally:
        if progress is not None:
            progress.close()

    return stats


def _download_one(
    client: EurostatClient, dest_root: Path, target: DownloadTarget
) -> Path:
    dest_path = dest_root / target.filename
    client.download_target(target, dest_path)
    return dest_path
