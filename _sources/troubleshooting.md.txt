# Troubleshooting

(isolated_environment_setup)=
## Set up an isolated environment (recommended setup)
### Pyenv
Pyenv is a tool used to manage different versions of Python on a single operating system (follow this [guide](https://github.com/pyenv/pyenv#installation) for installation instructions). If you are having problems getting Python 3.10 to work, we recommend you use Pyenv to set it up.

Once pyenv is installed, navigate to your project's directory and run the following commands:
```
pyenv install 3.10.4
pyenv local 3.10.4
```

The first command downloads Python version `3.10.4` on your operating system. The second command create a `.python-version` file in your current directory which will be used by pyenv so set the active Python version when you are working from that directory.

To test that the correct Python version has been selected, you can run the following command:
```bash
pyenv version
```
Which should output something like:
```
3.10.4 (set by /my/project/path/.python-version)
```

Note: At time of writing, `3.10.4` is the latest Python version.

### Poetry
Poetry is the recommended Python package manager to use with Subgrounds (follow this [guide](https://python-poetry.org/docs/master/#installing-with-the-official-installer) for installation instructions).

Once you have Poetry installed and your active Python version is `3.10.x`, initialize your project and install Subgrounds by running the following commands:
```
poetry init
poetry add subgrounds ipykernel
```

Note: The `ipykernel` might be needed to run Jupyter notebooks with Python `3.10.x`.

Now you're all set!

To run a Subgrounds powered Python program, you can use either of the following methods:
```
poetry run python my_subgrounds_project.py
```
Or
```
poetry shell
python my_subgrounds_project.py
```
