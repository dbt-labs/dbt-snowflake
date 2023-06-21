# Contributing to `dbt-snowflake`

1. [About this document](#about-this-document)
3. [Getting the code](#getting-the-code)
5. [Running `dbt-snowflake` in development](#running-dbt-snowflake-in-development)
6. [Testing](#testing)
7. [Updating Docs](#updating-docs)
7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide for anyone interested in contributing to the `dbt-snowflake` repository. It outlines how to create issues and submit pull requests (PRs).

This is not intended as a guide for using `dbt-snowflake` in a project. For configuring and using this adapter, see [Snowflake Profile](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile), and [Snowflake Configs](https://docs.getdbt.com/reference/resource-configs/snowflake-configs).

We assume users have a Linux or MacOS system. You should have familiarity with:

- Python `virturalenv`s
- Python modules
- `pip`
- common command line utilities like `git`.

In addition to this guide, we highly encourage you to read the [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md). Almost all information there is applicable here!

### Signing the CLA

Please note that all contributors to `dbt-snowflake` must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements)(CLA) before their pull request(s) can be merged into the `dbt-snowflake` codebase. Given this, `dbt-snowflake` maintainers will unfortunately be unable to merge your contribution(s) until you've signed the CLA. You are, however, welcome to open issues and comment on existing ones.

## Getting the code

 `git` is needed in order to download and modify the `dbt-snowflake` code. There are several ways to install Git. For MacOS, we suggest installing [Xcode](https://developer.apple.com/support/xcode/) or [Xcode Command Line Tools](https://mac.install.guide/commandlinetools/index.html).

### External contributors

If you are not a member of the `dbt-labs` GitHub organization, you can contribute to `dbt-snowflake` by forking the `dbt-snowflake` repository. For more on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt-snowflake` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request of your forked repository against `dbt-labs/dbt-snowflake`

### dbt Labs contributors

If you are a member of the `dbt Labs` GitHub organization, you will have push access to the `dbt-snowflake` repo. Rather than forking `dbt-snowflake` to make your changes, clone the repository like normal, and check out feature branches.

## Running `dbt-snowflake` in development

### Installation

1. Ensure you have the latest version of `pip` installed by running `pip install --upgrade pip` in terminal.

2. Configure and activate a `virtualenv` as described in [Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment).

3. Install `dbt-core` in the active `virtualenv`. To confirm you installed dbt correctly, run `dbt --version` and `which dbt`.

4. Install `dbt-snowflake` and development dependencies in the active `virtualenv`. Run `pip install -e . -r dev-requirements.txt`.

When `dbt-snowflake` is installed this way, any changes you make to the `dbt-snowflake` source code will be reflected immediately (i.e. in your next local dbt invocation against a Snowflake target).

## Testing

### Initial setup

`dbt-snowflake` contains [unit](https://github.com/dbt-labs/dbt-snowflake/tree/main/tests/unit) and [functional](https://github.com/dbt-labs/dbt-snowflake/tree/main/tests/functional) tests. Functional tests require an actual Snowflake warehouse to test against. There are two primary ways to do this:

- This repo has CI/CD GitHub Actions set up. Both unit and functional tests will run against an already configured Snowflake warehouse during PR checks.

- You can also run functional tests "locally" by configuring a `test.env` file with appropriate `ENV` variables.

```
cp test.env.example test.env
$EDITOR test.env
```

WARNING: The parameters in your `test.env` file must link to a valid Snowflake account. The `test.env` file you create is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing.


### "Local" test commands
There are a few methods for running tests locally.

#### `tox`
`tox` automatically runs unit tests against several Python versions using its own virtualenvs. Run `tox -p` to run unit tests for Python 3.8, Python 3.9, Python 3.10, and `flake8` in parallel. Run `tox -e py38` to invoke tests on Python version 3.8 only (use py38, py39, or py310). Tox recipes are found in `tox.ini`.

#### `pytest`
You may run a specific test or group of tests using `pytest` directly. Activate a Python virtualenv active with dev dependencies installed. Then, run tests like so:

```sh
# Note: replace $strings with valid names

# run all snowflake functional tests in a directory
python -m pytest tests/functional/$test_directory
# run all snowflake functional tests in a module
python -m pytest -m profile_snowflake tests/functional/$test_dir_and_filename.py
# run all snowflake functional tests in a class
python -m pytest -m profile_snowflake tests/functional/$test_dir_and_filename.py::$test_class_name
# run a specific snowflake functional test
python -m pytest -m profile_snowflake tests/functional/$test_dir_and_filename.py::$test_class_name::$test__method_name

# run all unit tests in a module
python -m pytest tests/unit/$test_file_name.py
# run a specific unit test
python -m pytest tests/unit/$test_file_name.py::$test_class_name::$test_method_name
```

## Updating documentation

Many changes will require an update to `dbt-snowflake` documentation. Here are some relevant links.

- Docs are [here](https://docs.getdbt.com/).
- The docs repo for making changes is located [here](https://github.com/dbt-labs/docs.getdbt.com).
- The changes made are likely to impact one or both of [Snowflake Profile](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile), or [Snowflake Configs](https://docs.getdbt.com/reference/resource-configs/snowflake-configs).
- We ask every community member who makes a user-facing change to open an issue or PR regarding doc changes.

## Adding CHANGELOG Entry

We use [changie](https://changie.dev) to generate `CHANGELOG` entries. **Note:** Do not edit the `CHANGELOG.md` directly. Your modifications will be lost.

Follow the steps to [install `changie`](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created, simply run `changie new` and changie will walk you through the process of creating a changelog entry.  Commit the file that's created and your changelog entry is complete!

You don't need to worry about which `dbt-snowflake` version your change will go into. Just create the changelog entry with `changie`, and open your PR against the `main` branch. All merged changes will be included in the next minor version of `dbt-snowflake`. The Core maintainers _may_ choose to "backport" specific changes in order to patch older minor versions. In that case, a maintainer will take care of that backport after merging your PR, before releasing the new version of `dbt-snowflake`.

## Submitting a Pull Request

A `dbt-snowflake` maintainer will review your PR and will determine if it has passed regression tests. They may suggest code revisions for style and clarity, or they may request that you add unit or functional tests. These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing and your PR has been approved, a `dbt-snowflake` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada:
