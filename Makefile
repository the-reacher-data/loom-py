.PHONY: test test-cov test-failfast test-node test-class mypy

TEST_NODE ?=
TEST_CLASS ?=

# Ejecuta toda la suite

test:
	uv run pytest -q

# Ejecuta tests con cobertura (sin depender de instalación previa de pytest-cov)

test-cov:
	uv run --with pytest-cov pytest -q --cov=src/loom --cov-report=term-missing --cov-report=xml

# Para en el primer fallo

test-failfast:
	uv run pytest -q -x

# Ejecuta un nodo concreto (archivo::Clase::test)
# Ejemplo: make test-node TEST_NODE=tests/integration/core/repository/sqlalchemy/test_repository_integration.py::TestRepositorySQLAlchemyIntegration

test-node:
	@if [ -z "$(TEST_NODE)" ]; then echo "Define TEST_NODE=<ruta::Clase::test>"; exit 1; fi
	uv run pytest -q "$(TEST_NODE)"

# Ejecuta todos los tests de una clase
# Ejemplo: make test-class TEST_CLASS=tests/integration/core/repository/sqlalchemy/test_repository_integration.py::TestRepositorySQLAlchemyIntegration

test-class:
	@if [ -z "$(TEST_CLASS)" ]; then echo "Define TEST_CLASS=<ruta::Clase>"; exit 1; fi
	uv run pytest -q "$(TEST_CLASS)"

# Validación de tipos

mypy:
	uv run mypy src tests
