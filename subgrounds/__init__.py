from subgrounds.subgraph import FieldPath, Subgraph, SyntheticField
from subgrounds.subgrounds import Subgrounds

__version__ = "1.6.1"

__all__ = [
    "FieldPath",
    "Subgraph",
    "Subgrounds",
    "SyntheticField",
]

### Pyodide Patch ###

from subgrounds.contrib import pyodide as _pyodide

_pyodide.patch()
