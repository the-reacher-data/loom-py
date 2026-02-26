.PHONY: test test-cov test-failfast test-node test-class mypy test-golden golden-update

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

# Ejecuta sólo los tests de golden testing
# Corre tests/unit/testing/test_golden.py y los conftest fixtures de tests/conftest.py

test-golden:
	uv run pytest -q tests/unit/testing/test_golden.py

# Regenera todos los snapshots golden (plans + outputs)
# Equivale a pasar --update-golden al suite completo

golden-update:
	uv run pytest -q --update-golden

# Validación de tipos

mypy:
	uv run mypy src tests
