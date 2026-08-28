"""Run-scoped logging shared by long-lived FaaSnap components.

The experiment driver atomically writes ``logging/ACTIVE_EXPERIMENT``.  Every
emit resolves that pointer, so services do not need to restart between
probability points and never keep writing into an older run's file handle.
"""

from datetime import datetime
import json
import logging
import os
from pathlib import Path
import re
import uuid

try:
    from . import config as runtime_config
except ImportError:
    import config as runtime_config


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGGING_ROOT = PROJECT_ROOT / 'logging'
ACTIVE_EXPERIMENT_FILE = LOGGING_ROOT / 'ACTIVE_EXPERIMENT'
_SAFE_COMPONENT = re.compile(r'[^A-Za-z0-9_.-]+')
_LOGGING_DISABLED_LEVEL = logging.CRITICAL + 1


def experiment_logging_enabled():
    return getattr(runtime_config, 'ENABLE_EXPERIMENT_LOGGING', True)


def _safe_component_name(component):
    return _SAFE_COMPONENT.sub('_', component).strip('._') or 'component'


def get_active_experiment_dir(logging_root=LOGGING_ROOT):
    pointer = Path(logging_root) / ACTIVE_EXPERIMENT_FILE.name
    try:
        run_id = pointer.read_text(encoding='utf-8').strip()
    except OSError:
        return None
    if not run_id or Path(run_id).name != run_id:
        return None
    directory = Path(logging_root) / run_id
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    return directory


def activate_experiment(workflow, probability, seed, metadata=None):
    LOGGING_ROOT.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_id = (
        f'{timestamp}_{workflow}_p{float(probability):.2f}_seed{seed}_'
        f'{uuid.uuid4().hex[:8]}'
    )
    run_dir = LOGGING_ROOT / run_id
    run_dir.mkdir(parents=False, exist_ok=False)
    manifest = {
        'run_id': run_id,
        'workflow': workflow,
        'configured_probability': float(probability),
        'retry_abort_seed': seed,
        'created_at': datetime.now().astimezone().isoformat(),
        **(metadata or {}),
    }
    (run_dir / 'manifest.json').write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')
    temporary_pointer = LOGGING_ROOT / f'.ACTIVE_EXPERIMENT.{os.getpid()}.tmp'
    temporary_pointer.write_text(run_id + '\n', encoding='utf-8')
    os.replace(temporary_pointer, ACTIVE_EXPERIMENT_FILE)
    return run_dir


class ExperimentFileHandler(logging.Handler):
    """Append each record to the currently active experiment directory."""

    def __init__(self, component, logging_root=LOGGING_ROOT):
        super().__init__(level=logging.INFO)
        self.component = _safe_component_name(component)
        self.logging_root = Path(logging_root)
        self._run_directory = None
        self._stream = None

    def _switch_stream(self, directory):
        if directory == self._run_directory and self._stream is not None:
            return
        if self._stream is not None:
            self._stream.close()
        self._run_directory = directory
        self._stream = None
        if directory is not None:
            self._stream = (directory / f'{self.component}.log').open(
                'a', encoding='utf-8')

    def emit(self, record):
        try:
            directory = get_active_experiment_dir(self.logging_root)
            self._switch_stream(directory)
            if self._stream is None:
                return
            self._stream.write(self.format(record) + '\n')
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def close(self):
        if self._stream is not None:
            self._stream.close()
            self._stream = None
        super().close()


def make_experiment_logger(name, component):
    logger = logging.getLogger(name)
    logger.propagate = False
    for existing_handler in logger.handlers:
        existing_handler.close()
    logger.handlers.clear()
    if not experiment_logging_enabled():
        logger.setLevel(_LOGGING_DISABLED_LEVEL)
        return logger
    logger.setLevel(logging.INFO)
    handler = ExperimentFileHandler(component)
    handler.setFormatter(logging.Formatter(
        '[%(asctime)s.%(msecs)03d] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    ))
    logger.addHandler(handler)
    return logger


def configure_root_experiment_logging(component):
    root_logger = logging.getLogger()
    for existing_handler in root_logger.handlers:
        existing_handler.close()
    root_logger.handlers.clear()
    if not experiment_logging_enabled():
        root_logger.setLevel(_LOGGING_DISABLED_LEVEL)
        return root_logger
    root_logger.setLevel(logging.INFO)
    handler = ExperimentFileHandler(component)
    handler.setFormatter(logging.Formatter(
        '[%(asctime)s.%(msecs)03d] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    ))
    root_logger.addHandler(handler)
    return root_logger
