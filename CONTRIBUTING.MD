# Contributing to `dbt-snowflake`

1. [About this document](#about-this-document)
3. [Getting the code](#getting-the-code)
5. [Running `dbt-snowflake` in development](#running-dbt-snowflake-in-development)
6. [Testing](#testing)
7. [Updating Docs](#updating-docs)
7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document
This document is a guide for anyone intersted in contributing to the `dbt-snowflake` repository. It outlines how to create issues and sumbit pull requests (PRs).

This is not intended as a guide for using `dbt-snowflake` in a project. For information on configuring and usage see [Snowflake Profile](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile), and [Snowflake Configs](https://docs.getdbt.com/reference/resource-configs/snowflake-configs).

We assume users have a Linux or MacOS system. You should have familiarity with:

- Python virturalenvs
- python modules
- `pip`
- common command line utilities liike `git`.

In addition to this guide, we  highly encourage you to readthe [dbt-core](https://github.com/dbt-labs/dbt-core/blob/main/CONTRIBUTING.md). Almost all of the information there is applicable to contributing here, too!

### Signing the CLA

Please note that all contributors to `dbt-snowflake` must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements) to have their Pull Request merged into an `dbt-snowflake` codebase. If you are unable to sign the CLA, then the `dbt-snowflake` maintainers will unfortunately be unable to merge your Pull Request. You are, however, welcome to open issues and comment on existing ones.

## Getting the code 

 `git` is needed in order to download and modify the `dbt-snowflake` code. There are several ways to install Git, for macOS we suggest installing [Xcode](https://developer.apple.com/support/xcode/) or [Xcode Command Line Tools](https://mac.install.guide/commandlinetools/index.html).

### External contributors

If you are not a member of the `dbt-labs` GitHub organization, you can contribute to `dbt-snowflake` by forking the `dbt-snowflake` repository. For more on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. fork the `dbt-snowflake` repository
2. clone your fork locally
3. check out a new branch for your proposed changes
4. push changes to your fork
5. open a pull request against `dbt-labs/dbt-snowflake` from your forked repository

### dbt Labs contributors

If you are a member of the `dbt Labs` GitHub organization, you will have push access to the `dbt-snowflake` repo. Rather than forking `dbt-snowflake` to make your changes, just clone the repository, check out a new branch, and push directly to that branch.

## Running `dbt-snowflake` in development

### Installation

1. First make sure you have the latest version of `pip` installed by running `pip install --upgrade pip` in the terminal. 

2. Configure and activate a `virtualenv` as described in [Setting up an environment](https://github.com/dbt-labs/dbt-core/blob/HEAD/CONTRIBUTING.md#setting-up-an-environment)

3. Install `dbt-core`. To confirm you are using the correct version installed run `dbt --version` and `which dbt`

4. Install `dbt-snowflake` and dependencies by running `pip install -e . -r dev-requirements.txt` in the same `virtual env`.  

When `dbt-snowflake` is installed this way, any changes you make to the `dbt-snowflake` source code will be reflected immediately in your next `dbt-snowflake` run.

## Testing

### Initial Setup

`dbt-snowflake` contains [unit](https://github.com/dbt-labs/dbt-snowflake/tree/main/tests/unit) and [integration](https://github.com/dbt-labs/dbt-snowflake/tree/main/tests/integration) tests. Integration tests require testing against an actual Snowflake warehouse. There are two major ways to run tests. 

- This repo has a CI/CD GithubAction to run tests against a Snowflake warehouse during PR checks.

- You can also run the integration tests locally by configuring a `test.env` file with the approprate `env` variables which you can obtain from `test.env.example` and corresponding credentials from your valid Snowflake account.

Note: This `test.env` file is git-ignored, but please be _extra_ careful to never check in credentials or other sensitive information when developing. To create your `test.env`.

```
cp test.env.example test.env
$EDITOR test.env
```

### Testing Locally Commands
There are a few methods for running tests locally.

#### `tox`
`tox` takes care of managing Python virtualenvs and installing dependencies in order to run tests. You can also run tests in parallel, for example you can run unit tests for Python 3.7, Python 3.8, Python 3.9, and `flake8` checks in parallel with `tox -p`. Also, you can run unit tests for specific python versions with `tox -e py37`. The configuration of these tests are located in `tox.ini`.

#### `pytest`
You can also run a specific test or group of tests using `pytest` directly. With a Python virtualenv active and dev dependencies installed you can run test commands like:

```sh
# run specific snowflake integration tests
python -m pytest -m profile_snowflake tests/integration/simple_copy_test
# run all unit tests in a file
python -m pytest tests/unit/test_snowflake_adapter.py
# run a specific unit test
python -m pytest tests/unit/test_snowflake_adapter.py::TestSnowflakeAdapter::test_convert_date_type
```
## Updating Docs

Many changes will require an update to `dbt-snowflake` documentation. Here are some relevant links.

- Docs are [here](https://docs.getdbt.com/).
- The docs repo for making changes is located [here]( https://github.com/dbt-labs/docs.getdbt.com).
- The changes made are likely to impact one or both of [Snowflake Profile](https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile), or [Snowflake Configs](https://docs.getdbt.com/reference/resource-configs/snowflake-configs).
- We ask every community member who makes a user-facing change to open an issue or PR regarding doc changes.

## Submitting a Pull Request

dbt Labs provides a CI/CD environment to test changes to the `dbt-snowflake` adapter, Which will trigger on push as part of review criteria against contributor code. Tests are run against the development version of `dbt-core` through Github Actions.

A `dbt-snowflake` maintainer will review your PR and will decide if its approved testing in our CI. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Once all tests are passing and your PR has been approved, a `dbt-snowflake` maintainer will merge your changes into the active development branch. And that's it! Happy developing :tada: