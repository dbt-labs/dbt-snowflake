.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@pip install -e ".[dev,test]" && pre-commit install

.PHONY: dev-uninstall
dev-uninstall: ## Uninstalls all packages while maintaining the virtual environment
	@pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y
	@pip uninstall -y dbt-snowflake

.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes
	@pre-commit run black | grep -v "INFO"
	@pre-commit run flake8 | grep -v "INFO"
	@pre-commit run mypy | grep -v "INFO"

.PHONY: unit
unit: ## Runs unit tests with py38
	@tox -e py38

.PHONY: integration
integration: ## Runs snowflake integration tests with py38
	@tox -e py38-snowflake --

.PHONY: clean
clean:  ## Remove all untracked files, but keep the credentials file
	@git clean -X -n --exclude="!test.env"

.PHONY: help
help: ## Show this help message
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: ubuntu-py38
ubuntu-py38:
	@docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py38 . --build-arg version=3.8
	@docker run --rm -it --name dbt-snowflake-ubuntu-py38 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py38

.PHONY: ubuntu-py39
ubuntu-py39:
	@docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py39 . --build-arg version=3.9
	@docker run --rm -it --name dbt-snowflake-ubuntu-py39 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py39

.PHONY: ubuntu-py310
ubuntu-py310:
	@docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py310 . --build-arg version=3.10
	@docker run --rm -it --name dbt-snowflake-ubuntu-py310 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py310

.PHONY: ubuntu-py311
ubuntu-py311:
	@docker build -f docker_dev/ubuntu.Dockerfile -t dbt-snowflake-ubuntu-py311 . --build-arg version=3.11
	@docker run --rm -it --name dbt-snowflake-ubuntu-py311 -v $(shell pwd):/opt/code dbt-snowflake-ubuntu-py311
