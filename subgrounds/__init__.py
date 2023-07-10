from subgrounds.client import AsyncSubgrounds, Subgrounds
from subgrounds.subgraph import FieldPath, Subgraph, SyntheticField

__version__ = "1.6.0"

__all__ = [
    "AsyncSubgrounds",
    "FieldPath",
    "Subgraph",
    "Subgrounds",
    "SyntheticField",
]

### Pyodide Patch ###

from subgrounds.contrib import pyodide as _pyodide

_pyodide.patch()
