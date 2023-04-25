# Changelog

<!--next-version-placeholder-->

## v1.5.0 (2023-04-25)
### Feature
* Add `all` as an extra install ([`f1adacf`](https://github.com/0xPlaygrounds/subgrounds/commit/f1adacf8162e711a8395cd75c9726350ec67c0c5))
* `subgrounds[pyodide]` patching for requests ([`7293262`](https://github.com/0xPlaygrounds/subgrounds/commit/7293262bed062932a6ca52ae69cf0545850ed470))
* Add `plotly` as an extra ([`058e28d`](https://github.com/0xPlaygrounds/subgrounds/commit/058e28d2f4170fabd0558313be50137ecd8c4972))

### Fix
* `poetry.lock` file ([`5e365e0`](https://github.com/0xPlaygrounds/subgrounds/commit/5e365e09e63e5a077fcca861b6ecb0cbb494cc0f))
* Enable `pyodide` patch on import ([`dd5193b`](https://github.com/0xPlaygrounds/subgrounds/commit/dd5193b2a46ff16ed4e7c7ca8f5d708bfa65d63b))

## v1.4.0 (2023-04-13)
### Feature
* `subgrounds.contrib.dash` and `subgrounds.contrib.plotly` ([`f1d79e8`](https://github.com/0xPlaygrounds/subgrounds/commit/f1d79e89f1594bc31d443fa2b248f745974dd468))

### Fix
* Headers bugs w/ non-subgraph apis ([`7749f8f`](https://github.com/0xPlaygrounds/subgrounds/commit/7749f8f378fd5dcad727e2b507a50ed99654fab3))
* Update examples to user modern subgrounds ([`6e14b89`](https://github.com/0xPlaygrounds/subgrounds/commit/6e14b89dd040d938210827d2defcfa80e397a896))

### Documentation
* Adjust wording ([`501b480`](https://github.com/0xPlaygrounds/subgrounds/commit/501b480a93834978faa063633060389ef82b5931))
* Add extra details about `contrib` and deprecation ([`6a7bbec`](https://github.com/0xPlaygrounds/subgrounds/commit/6a7bbecbbd30d926ff7a9c8285b3c7be64b28722))

## v1.3.0 (2023-04-07)
### Feature
* Add specific error models ([`6218076`](https://github.com/0xPlaygrounds/subgrounds/commit/621807602c3467a3e729541f645d992d3e5b6297))

## v1.2.0 (2023-04-02)
### Feature
* Headers Support ([#13](https://github.com/0xPlaygrounds/subgrounds/issues/13)) ([`8ff3c04`](https://github.com/0xPlaygrounds/subgrounds/commit/8ff3c040a2c8c205b31566f1135e38a3c84293b2))

### Documentation
* Fix changelog formatting ([`7f8a630`](https://github.com/0xPlaygrounds/subgrounds/commit/7f8a6302871387042ffa0ea323284832f2611ea7))

## v1.1.2 (2023-03-15)
### Fix
* `Query.map` had no default value for `priority` ([#12](https://github.com/0xPlaygrounds/subgrounds/issues/12)) ([`82a5f29`](https://github.com/0xPlaygrounds/subgrounds/commit/82a5f293e4cf7398350d12e309cd3cff190c1318))

### Documentation
* Update README.md ([`dcea873`](https://github.com/0xPlaygrounds/subgrounds/commit/dcea87388020ef8465c0752e0ae79469d1f018e1))
* Update design goals to features ([`6abecc6`](https://github.com/0xPlaygrounds/subgrounds/commit/6abecc68b3d9f573bf6feb6c14c0e0107fc7e8a7))

## v1.1.1 (2023-03-03)
### Fix
* Bump version ([`40004c0`](https://github.com/0xPlaygrounds/subgrounds/commit/40004c012e474a32c94812576e24069c8a4c5d51))

## v1.1.0 (2023-03-03)

### Feature
* Export more symbols to `subgrounds` ([`3c59fe9`](https://github.com/0xPlaygrounds/subgrounds/commit/3c59fe9e0f96947d23905a0eab384c01cf8d42f7))

### Documentation
* Add description about testing ([`f2231c2`](https://github.com/0xPlaygrounds/subgrounds/commit/f2231c2b02fe0194409b91d865dedd6d45ecd92c))
* Migrated `docs/` and clean up ([`9deca3b`](https://github.com/0xPlaygrounds/subgrounds/commit/9deca3bc48a38018c9baa73ba625ec37decf1670))
* Add CONTRIBUTING.md + README.md edits ([`2218d47`](https://github.com/0xPlaygrounds/subgrounds/commit/2218d47df13bf2cc5a43950f1106b62c20a16d5f))
* Trigger docs flow (removed some files) ([`1ee17e5`](https://github.com/0xPlaygrounds/subgrounds/commit/1ee17e5129192c3c202830f929f284c8ca019650))
* Simplify contributing cmds ([#3](https://github.com/0xPlaygrounds/subgrounds/issues/3)) ([`0b4f3a2`](https://github.com/0xPlaygrounds/subgrounds/commit/0b4f3a2590a3e916e614bff97b578bb32d5097e2))

## v1.0.3 (2022-08-08)

### Fix
- Issue causing null object in `SyntheticField`s


## v1.0.2 (2022-10-30)
### Fix
- Update `SchemaMeta` to `Pydantic` model for extra validation

## v1.0.1 (2022-08-08)

### Fix
- Schema cache directory path
- `where` filter getting dropped during pagination


## v1.0.0 (2022-07-25)

### Description
This release overhauls how pagination is performed in Subgrounds.

### BREAKING CHANGES
* The `auto_paginate` argument of toplevel querying functions has been replaced by the `pagination_strategy` argument (setting the latter to `None` disables pagination).

### Feature
* Subgrounds users can now define and use their own pagination strategy (see Custom pagination strategy for more details).
* Users select the pagination strategies to use when calling toplevel querying functions:

    ```py
    df = sg.query_df(field_paths, pagination_strategy=MyPaginationStrategy) 
    ```

* Subgrounds provides two pagination strategies out of the box:
  * `LegacyStrategy` (the strategy used prior to this update)
  * `ShallowStrategy`, which is much faster than `LegacyStrategy` in some cases, but does not support pagination on nested fields (see Available pagination strategies for more details).

### Fix
* Fix bug that would cause a crash when calling one of the toplevel querying functions with only one `FieldPath` object (e.g.: `sg.query_df(pairs.id)`).


## v0.2.0 (2023-06-23)

### Feature
* Iterative versions of toplevel querying functions to allow developers to process queried data page by page when automatic pagination returns more than one page
  * `query_df` -> `query_df_iter`
  * `query` -> `query_iter`
  * `query_json` -> `query_json_iter`
  * `execute` -> `execute_iter`
* Add option to set subgraph schema cache directory in `load_subgraph` and `load_api`.
  * Defaults to `./schemas/`
* Add useful `SyntheticField` helper `datetime_of_timestamp`
* `Subgrounds` class can now be imported from toplevel module: `from subgrounds import Subgrounds`
* Add `SyntheticField.datetime_of_timestamp` helper function to easily create a `SyntheticField` that converts a Unix timestamp into a human readable datetime string
* Add `SyntheticField.map` helper function to easily create a "map" `SyntheticField` from a dictionary

### Fix
* Fix bug that caused some queries to fail with automatic pagination enabled

### Chore
* Migrate package manager from `pipenv` to `poetry`
* Migrate docs from plain `sphinx` to `mudkip`
* Made `dash` an optional extra dependency.
  * To use Subgrounds `dash` and `plotly` wrappers, run `pip install subgrounds[dash]`


## v0.1.1 (2022-04-29)

### Fix
- Scalar lists were not handled properly

## v0.1.0 (2022-04-27)

### Feature
- Partial fieldpaths
  - Querying partial fieldpaths will automatically select all non-list fields of the fieldpath leaf.
  - For example, using the Ribbon V2 subgraph, the following Subgrounds query:

    ```py
    sg.query_df([
        ribbon.Query.vaults
    ])
    ```

    will result in the following query being made to the subgraph:

    ```graphql
    query {
        vaults {
        id
        name
        symbol
        underlyingAsset
        underlyingName
        underlyingSymbol
        underlyingDecimals
        totalPremiumEarned
        totalNominalVolume
        totalNotionalVolume
        numDepositors
        totalBalance
        cap
        round
        performanceFeeCollected
        managementFeeCollected
        totalFeeCollected
        }
    }
    ```

- Code completion
  - When using Subgrounds in Jupyter notebooks and a subgraph has been loaded in a previous cell, fieldpaths will now autocomplete.


## v0.0.9 (2023-03-29)

### Feature
* `Subgrounds.load_api`
  * Allows you to safely query non-subgraph GrahpQL APIs with Subgrounds!
* New boolean flag argument, `auto_paginate`, in `Subgrounds.query`, `Subgrounds.query_json` and `Subgrounds.query_df`
  * Selectively disable automatic pagination

### Documentation
* Refactor, cleanup and add tons of docstrings
