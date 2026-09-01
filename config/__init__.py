"""Project configuration package.

Keeping the values in ``config.config`` is backward compatible with existing
``from config import config`` callers.  Re-exporting them here also makes a
plain ``import config`` unambiguous once the repository root is on ``sys.path``.
"""

from .config import *
