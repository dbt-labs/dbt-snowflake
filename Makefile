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

.PHONY: unit
unit: ## Runs unit tests with py38.
	@\
	tox -e py38

.PHONY: test
test: ## Runs unit tests with py38 and code checks against staged changes.
	@\
	tox -p -e py38; \
	pre-commit run --all-files

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

.PHONY: docker-dev
docker-dev:
	docker build -f docker/dev.Dockerfile -t dbt-snowflake-dev .
	docker run --rm -it --name dbt-snowflake-dev -v $(shell pwd):/opt/code dbt-snowflake-dev

.PHONY: docker-prod
docker-prod:
	docker build -f docker/Dockerfile -t dbt-snowflake .
