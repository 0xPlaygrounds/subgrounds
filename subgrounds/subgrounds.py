""" Toplevel Subgrounds module

This module implements the toplevel API that most developers will be using when
querying The Graph with Subgrounds.
"""

import warnings

from .client import Subgrounds

__all__ = ["Subgrounds"]

warnings.warn(
    "Importing from `subgrounds.subgrounds` is deprecated."
    " Use `subgrounds` instead.\n"
    "Will be removed in a future version.",
    DeprecationWarning,
)
