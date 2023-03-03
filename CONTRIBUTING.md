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

<details>
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
```bash
$ pytest
```

## Code Style
> ⚠️ TODO -> Use Black

```bash
$ poe format
$ poe check
```
