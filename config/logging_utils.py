import os
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional


_ACTIVE_RUN_FILE = ".active_run_id"
_ACTIVE_EXPERIMENT_FILE = ".active_experiment"
_DEFAULT_TTL_SECONDS = 300


def _read_text(path: Path):
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _new_run_id():
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"


def _normalize_subdir(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return ""
    sanitized = name.replace("..", "_").replace("\\", "_").replace("/", "_")
    return sanitized.strip("._")


def resolve_log_run_id(root_dir: Path, ttl_seconds: int = _DEFAULT_TTL_SECONDS):
    """
    Resolve a log run id shared by processes started in a short time window.
    Priority:
    1) FAASNAP_LOG_RUN_ID env var (explicit)
    2) logging/.active_run_id if fresh
    3) create a new run id
    """
    env_run_id = os.environ.get("FAASNAP_LOG_RUN_ID", "").strip()
    logging_root = Path(root_dir) / "logging"
    active_file = logging_root / _ACTIVE_RUN_FILE

    if env_run_id:
        _write_text(active_file, env_run_id)
        return env_run_id

    if active_file.exists():
        run_id = _read_text(active_file)
        if run_id:
            age = time.time() - active_file.stat().st_mtime
            run_dir = logging_root / "runs" / run_id
            if age <= ttl_seconds and run_dir.exists():
                return run_id

    run_id = _new_run_id()
    _write_text(active_file, run_id)
    return run_id


def resolve_log_experiment(root_dir: Path):
    """
    Resolve current experiment sub-directory for logs.
    Priority:
    1) FAASNAP_LOG_EXPERIMENT env var (explicit)
    2) logging/.active_experiment file
    3) empty (write directly under run dir)
    """
    logging_root = Path(root_dir) / "logging"
    active_file = logging_root / _ACTIVE_EXPERIMENT_FILE

    env_experiment = _normalize_subdir(os.environ.get("FAASNAP_LOG_EXPERIMENT", ""))
    if env_experiment:
        _write_text(active_file, env_experiment)
        return env_experiment

    file_experiment = _normalize_subdir(_read_text(active_file))
    return file_experiment


def set_active_log_experiment(root_dir: Path, experiment: str):
    logging_root = Path(root_dir) / "logging"
    active_file = logging_root / _ACTIVE_EXPERIMENT_FILE
    _write_text(active_file, _normalize_subdir(experiment))


def get_run_log_dir(root_dir: Path, ttl_seconds: int = _DEFAULT_TTL_SECONDS, experiment: Optional[str] = None):
    run_id = resolve_log_run_id(root_dir, ttl_seconds=ttl_seconds)
    run_dir = Path(root_dir) / "logging" / "runs" / run_id
    if experiment is None:
        experiment = resolve_log_experiment(root_dir)
    experiment = _normalize_subdir(experiment)
    if experiment:
        run_dir = run_dir / experiment
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def get_component_log_path(root_dir: Path, filename: str, ttl_seconds: int = _DEFAULT_TTL_SECONDS, experiment: Optional[str] = None):
    return get_run_log_dir(root_dir, ttl_seconds=ttl_seconds, experiment=experiment) / filename


class RunAwareFileHandler(logging.Handler):
    """
    File handler that follows the active run id.
    It transparently switches target file when logging/.active_run_id changes.
    """

    def __init__(self, root_dir: Path, filename: str, ttl_seconds: int = _DEFAULT_TTL_SECONDS, mode: str = "a", encoding: str = "utf-8", experiment: Optional[str] = None):
        super().__init__()
        self._root_dir = Path(root_dir)
        self._filename = filename
        self._ttl_seconds = ttl_seconds
        self._mode = mode
        self._encoding = encoding
        self._experiment = experiment
        self._handler = None
        self._current_path = None

    def _ensure_handler(self):
        target_path = str(
            get_component_log_path(
                self._root_dir,
                self._filename,
                ttl_seconds=self._ttl_seconds,
                experiment=self._experiment,
            )
        )
        if self._handler is not None and target_path == self._current_path:
            return

        if self._handler is not None:
            self._handler.close()

        self._handler = logging.FileHandler(target_path, mode=self._mode, encoding=self._encoding)
        self._handler.setLevel(self.level)
        if self.formatter is not None:
            self._handler.setFormatter(self.formatter)
        self._current_path = target_path

    def emit(self, record):
        try:
            self._ensure_handler()
            self._handler.emit(record)
        except Exception:
            self.handleError(record)

    def setFormatter(self, fmt):
        super().setFormatter(fmt)
        if self._handler is not None:
            self._handler.setFormatter(fmt)

    def setLevel(self, level):
        super().setLevel(level)
        if self._handler is not None:
            self._handler.setLevel(level)

    def close(self):
        try:
            if self._handler is not None:
                self._handler.close()
                self._handler = None
        finally:
            super().close()
