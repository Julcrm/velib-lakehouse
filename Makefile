.PHONY: all install add add-dev run test lint clean
# Vérifie si uv est installé
UV := $(shell command -v uv 2> /dev/null)

check_uv:
ifndef UV
	$(error "uv n'est pas installé !")
endif

install:
	uv sync

add:
	@test -n "$(lib)" || (echo "Usage: make add lib=pandas" && exit 1)
	uv add $(lib)

add-dev:
	@test -n "$(lib)" || (echo "Usage: make add-dev lib=pytest" && exit 1)
	uv add --dev $(lib)

run:
	uv run src/main.py

test:
	uv run pytest tests/

lint:
	uv run ruff check --fix .

clean:
	rm -rf .venv
	rm -rf build dist
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache