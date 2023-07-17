# Contributing

We actively encourage contributions. Feel free to open an issue to discuss changes or open a pull request if you want to make changes to our [Github](https://github.com/0xPlaygrounds/subgrounds).

## Setup

This project uses [poetry >= 1.2](https://python-poetry.org/docs/) to manage it's dependencies. Please checkout the official instructions to setup poetry on your system.

```bash
$ git clone https://github.com/0xPlaygrounds/subgrounds && cd subgrounds
$ poetry install
# or
$ poetry install --all-extras
```

We also use a `poetry` plugin called `poethepoet` to aid in managing our frequently run tasks.

```bash
$ poetry self add 'poethepoet[poetry_plugin]'
```

<details class="admonition hint dropdown toggle-hidden" style="padding: 0.75em">
<summary>Alternative to using <code>poe</code></summary>
<br>

If you wish not use `poe`, you'll have to run the following and checkout the commands listed in the `pyproject.toml` under `[tool.poe.tasks]`.

This will run `<my-command>` inside a virtual environment (`poetry shell` will explictly keep you inside one until you run `deactivate`)

```bash
$ poetry shell
$ <my-command>
$ deactivate
# or
$ poetry run <my-command>
```
</details>


## Testing

We use `pytest` to ensure our code doesn't regress in behavior with fixes and new features. Make sure to continously run tests alongside writing new ones as you add more features to the codebase.

```bash
$ poe test
```

## Code Style

We use [black](https://github.com/psf/black) and [ruff](https://github.com/charliermarsh/ruff) to maintain our code style.

- Black provides us an opinionated set of style conventions which helps keep the code consistent.
- Ruff is a bleeding edge `flake8` replacement which helps us lint our code efficiently and effectively.

This library also leverages python typing via the pyright project. We leverage the type-safety via the official [Python VSCode Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python).

All PRs should be type-checked and adhere to the `black` and `ruff` style conventions.

```bash
$ poe format
$ poe check
```
