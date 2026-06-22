.DEFAULT_GOAL := help
.PHONY: help install hooks lint format typecheck security test cov check up down logs migrate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install dev dependencies
	pip install -r requirements/dev.txt

hooks: ## Install pre-commit git hooks
	pre-commit install

lint: ## Run ruff lint
	ruff check .

format: ## Format code with ruff
	ruff format .

typecheck: ## Run mypy on src
	mypy src --ignore-missing-imports

security: ## Run bandit + pip-audit
	bandit -c pyproject.toml -r src --severity-level medium
	pip-audit -r requirements/base.txt

test: ## Run the test suite
	python -m pytest tests/ -v --tb=short

cov: ## Run tests with coverage report
	python -m pytest tests/ --cov=src --cov-report=term-missing

check: lint typecheck test ## Run lint, typecheck and tests (CI parity)

up: ## Start the full stack
	docker compose up -d

down: ## Stop the stack
	docker compose down

logs: ## Tail the API logs
	docker compose logs -f api

migrate: ## Apply database migrations
	docker compose exec api alembic upgrade head
