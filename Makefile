.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@pip install --upgrade pip
	@pip install -e . -r dev-requirements.txt && pre-commit install

.PHONY: lint
lint: ## Runs linters (black, flake8, and mypy) against staged changes
	@pre-commit run --all-files | grep -v "INFO"

.PHONY: unit
unit: ## Runs unit tests with py38.
	@tox -e py38

.PHONY: integration
integration: ## Runs snowflake integration tests with py38.
	@tox -e py38-snowflake --

.PHONY: clean
clean: ## Resets git and removes directories created for linting and testing
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: dev-uninstall
dev-uninstall: ## Uninstalls all packages while maintaining the virtual environment
               ## Useful when updating versions, or if you accidentally installed into the system interpreter
	@pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y

platform = ubuntu
pyversion = 3.8
image = dbt-snowflake-${platform}-py${pyversion}

.PHONY: docker-dev
docker-dev:  ## Build and run a docker image on ubuntu or debian with python 3.8, 3.9, 3.10, or 3.11
	@echo 'usage: make docker-dev [pyversion=*3.8,3.9,3.10,3.11] [platform=*ubuntu,debian]'
	@echo
	@docker build -f docker_dev/${platform}.Dockerfile -t ${image} . --build-arg version=${pyversion}
	@docker run --rm -it --name ${image} -v $(shell pwd):/opt/code ${image}

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
