.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	pip install -e . -r dev-requirements.txt && pre-commit install

.PHONY: dev-uninstall
dev-uninstall: ## Uninstalls all packages while maintaining the virtual environment
               ## Useful when updating versions, or if you accidentally installed into the system interpreter
	pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y
	pip uninstall -y dbt-snowflake

.PHONY: mypy
mypy: ## Runs mypy against staged changes for static type checking.
	@\
	pre-commit run --hook-stage manual mypy-check | grep -v "INFO"

.PHONY: flake8
flake8: ## Runs flake8 against staged changes to enforce style guide.
	@\
	pre-commit run --hook-stage manual flake8-check | grep -v "INFO"

.PHONY: black
black: ## Runs black  against staged changes to enforce style guide.
	@\
	pre-commit run --hook-stage manual black-check -v | grep -v "INFO"

.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes.
	@\
	pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: linecheck
linecheck: ## Checks for all Python lines 100 characters or more
	@\
	find dbt -type f -name "*.py" -exec grep -I -r -n '.\{100\}' {} \;

.PHONY: unit
unit: ## Runs unit tests with py38.
	@\
	tox -e py38

.PHONY: test
test: ## Runs unit tests with py38 and code checks against staged changes.
	@\
	tox -p -e py38; \
	pre-commit run black-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: integration
integration: ## Runs snowflake integration tests with py38.
	@\
	tox -e py38-snowflake --

.PHONY: clean
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: ubuntu-py38
ubuntu-py38:
	docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py38 . --build-arg version=3.8
	docker run --rm -it --name dbt-snowflake-ubuntu-py38 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py38

.PHONY: ubuntu-py39
ubuntu-py39:
	docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py39 . --build-arg version=3.9
	docker run --rm -it --name dbt-snowflake-ubuntu-py39 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py39

.PHONY: ubuntu-py310
ubuntu-py310:
	docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py310 . --build-arg version=3.10
	docker run --rm -it --name dbt-snowflake-ubuntu-py310 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py310

.PHONY: ubuntu-py311
ubuntu-py311:
	docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py311 . --build-arg version=3.11
	docker run --rm -it --name dbt-snowflake-ubuntu-py311 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py311

.PHONY: debian-py38
debian-py38:
	docker build -f docker/debian-py38.Dockerfile -t dbt-snowflake-debian-py38 . --build-arg version=3.8.15
	docker run --rm -it --name dbt-snowflake-debian-py38 -v $(shell pwd):/opt/code dbt-snowflake-debian-py38

.PHONY: dev-env-default
dev-env-default:
	docker build -f docker/dev-env-default.Dockerfile -t dbt-snowflake-dev-env-default .
	docker run --rm -it --name dbt-snowflake-dev-env-default -v $(shell pwd):/opt/code dbt-snowflake-dev-env-default
