"""Pyodide Patch

`patch` uses `pyodide-http` to patch `requests` to work in `pyodide` environemnts.
"""

import sys
import warnings

__all__ = ["patch"]

def patch():
    """Attempts to patch `requests` to allow `subgrounds` to work in `pyodide`
    
    Outputs warnings instead of exceptions when things go wrong
    """
    
    if "pyodide" in sys.modules:
        try:
            import pyodide_http
        except ImportError:
            warnings.warn(
                "`pyodide-http` failed to import in the auto-detected `pyodide`"
                " environment. Did you install `subgrounds` with `pyodide` support?\n\n"
                "    `pip install subgrounds[pyodide]`"
            )
            return
        
        try:
            pyodide_http.patch_all()
        except Exception as e:
            warnings.warn(
                "Failed to patch with `pyodide-http`:\n"
                + str(e)
            )
