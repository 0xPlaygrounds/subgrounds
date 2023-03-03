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

If you wish to note use `poe`, you'll need to either run the commands manually with `poetry run` preceding, or you can explictly run `poetry shell` to launch a shell inside the virtual environment.

```bash
$ poetry shell
```

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
