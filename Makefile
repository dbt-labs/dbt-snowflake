# TODO: to implement
# .DEFAULT_GOAL:=help
# Optional flag to run target in a docker container.
# (example `make test USE_DOCKER=true`)
# ifeq ($(USE_DOCKER),true)
# endif

.PHONY: dev
dev: ## Installs adapter in develop mode along with development depedencies
	@\
	pip install -r dev_requirements.txt && pre-commit install

.PHONY: mypy
mypy: ## Runs mypy against staged changes for static type checking.
	@\
	$(DOCKER_CMD) pre-commit run --hook-stage manual mypy-check | grep -v "INFO"

.PHONY: flake8
flake8: ## Runs flake8 against staged changes to enforce style guide.
	@\
	$(DOCKER_CMD) pre-commit run --hook-stage manual flake8-check | grep -v "INFO"

.PHONY: black
black: ## Runs black  against staged changes to enforce style guide.
	@\
	$(DOCKER_CMD) pre-commit run --hook-stage manual black-check -v | grep -v "INFO"


.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes.
	@\
	$(DOCKER_CMD) pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	$(DOCKER_CMD) pre-commit run mypy-check --hook-stage manual | grep -v "INFO"


.PHONY: unit
unit: ## Runs unit tests with py38.
	@\
	$(DOCKER_CMD) tox -e py38

.PHONY: test
test: ## Runs unit tests with py38 and code checks against staged changes.
	@\
	$(DOCKER_CMD) tox -p -e py38; \
	$(DOCKER_CMD) pre-commit run black-check --hook-stage manual | grep -v "INFO"; \
	$(DOCKER_CMD) pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	$(DOCKER_CMD) pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: integration
integration: ## Runs postgres integration tests with py38.
	@\
	$(DOCKER_CMD) tox -e py38-postgres --

.PHONY: clean
	@echo "cleaning repo"
	@git clean -f -X
