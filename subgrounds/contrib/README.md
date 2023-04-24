# Subgrounds Contrib
> Extra parts of subgrounds that may not fit in the main package

## What is this?
Contrib is a niche concept in some libraries that represent extra / contributed content to a library that may not fit in the main package. This might be due to the maintainer not willing to maintain said content, the content being deemed too experimental, or perhaps it's unknown whether it's a "good idea" or not.

> Relevant [Stackoverflow](https://softwareengineering.stackexchange.com/questions/252053/whats-in-the-contrib-folder) post

For us, `subgrounds.contrib` will represent extra features and ideas with `subgrounds` that generally builds upon the core part of `subgrounds`. It allows us to add extensions or features to other libraries (such as `plotly`) without *relying* on the library as a dependency. We might add new concepts to this subpackage in the future, so look out!

## What's currently here?

### Plotly
```bash
pip install subgrounds[dash]
```

Originally located in `subgrounds.plotly_wrappers`, `subgrounds.contrib.plotly` contains helpful wrappers on `plotly` objects that allow you to use `FieldPaths` directly without creating a `pandas` `DataFrame`.

### Dash
```bash
pip install subgrounds[plotly]
```

Originally located in `subgrounds.dash_wrappers`, `subgrounds.contrib.dash` contains helpful wrappers on `dash` objects that allow you to use other wrapped visualization objects (currently `subgrounds.contrib.plotly`) in `dash` apps without creating `pandas` `DataFrame`s.

### Pyodide
```bash
pip install subgrounds[pyodide]
```

This module enables support for `pyodide` environments. This allows `subgrounds` to execute requests when used in programs such as `pyscript` and `jupyterlite`.

This `contrib` is imported and ran in `__init__` which means the support will be automatically enabled when `subgrounds` is important in a `pyodide` environment.
