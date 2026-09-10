"""Integration suite for ``app.rest.interfaces`` (spec 014, T501-T504).

Drives :func:`loom.rest.fastapi.auto.create_app` over a real, temporary
project. One class per property under test, following the AI integration
suite's layout.

See ``docs/rest/use-case-dsl.md`` for the documented example this module
also serves as the executable copy of (two-way pointer).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from loom.core.identity import Identity, reset_identity, set_identity
from loom.rest.autocrud import build_auto_routes
from loom.rest.compiler import InterfaceCompilationError
from loom.rest.config import RestInterfaceConfigError
from loom.rest.fastapi.auto import create_app

from .conftest import module_name, write_project


def _routes_under(app: FastAPI, prefix: str) -> dict[tuple[str, str], tuple[int, tuple[str, ...]]]:
    """Map ``(method, path suffix)`` -> ``(status_code, tags)`` for *prefix*.

    Only inspects ``APIRoute`` instances mounted under *prefix*, one entry
    per HTTP method a route answers to.
    """
    result: dict[tuple[str, str], tuple[int, tuple[str, ...]]] = {}
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.path.startswith(prefix):
            continue
        suffix = route.path[len(prefix) :] or "/"
        for method in route.methods or ():
            tags = tuple(str(tag) for tag in route.tags)
            result[(str(method), suffix)] = (route.status_code or 200, tags)
    return result


def _import_fixture_module(module: str, tmp_path: Path) -> Any:
    """Import the fixture module :func:`write_project` wrote under *tmp_path*."""
    if str(tmp_path) not in sys.path:
        sys.path.insert(0, str(tmp_path))
    return importlib.import_module(module)


class TestPythonAndYamlProduceTheSamePublishedApi:
    """A Python interface and its YAML equivalent mount the same routes."""

    def test_yaml_route_matches_the_python_route_it_mirrors(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "tickets_yaml": {
                        "prefix": "/tickets-yaml",
                        "tags": ["Tickets"],
                        "routes": [
                            {
                                "use_case": f"{module}:CreateTicketUseCase",
                                "method": "POST",
                                "path": "/",
                                "status_code": 201,
                            },
                            {
                                "use_case": f"{module}:GetTicketUseCase",
                                "method": "GET",
                                "path": "/{ticket_id}",
                            },
                        ],
                    }
                }
            },
        )
        app = create_app(config_path)

        python_routes = _routes_under(app, "/tickets-py")
        yaml_routes = _routes_under(app, "/tickets-yaml")

        assert python_routes == yaml_routes
        assert python_routes.keys() == {("POST", "/"), ("GET", "/{ticket_id}")}


class TestYamlAutoCrudMatchesTheGenerator:
    """An ``auto: true`` YAML interface mounts exactly the generated CRUD routes."""

    def test_generated_routes_match_build_auto_routes(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {"prefix": "/gadgets", "auto": True, "model": f"{module}:Gadget"}
                }
            },
        )
        app = create_app(config_path)
        gadget = _import_fixture_module(module, tmp_path).Gadget

        mounted = {key: value[0] for key, value in _routes_under(app, "/gadgets").items()}
        expected = {
            (route.method.upper(), route.path): route.status_code
            for route in build_auto_routes(gadget, ())
        }
        assert mounted == expected

    def test_a_live_create_and_get_round_trip_through_the_generated_routes(
        self, tmp_path: Path
    ) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {"prefix": "/gadgets", "auto": True, "model": f"{module}:Gadget"}
                }
            },
        )
        app = create_app(config_path)
        with TestClient(app) as client:
            created = client.post("/gadgets/", json={"name": "wrench"})
            assert created.status_code == 201, created.text
            gadget_id = created.json()["id"]
            fetched = client.get(f"/gadgets/{gadget_id}")
            assert fetched.status_code == 200
            assert fetched.json()["name"] == "wrench"


class TestCrossOriginCollisionAbortsNamingBoth:
    """A ``(method, path)`` shared by Python and YAML aborts startup."""

    def test_collision_names_both_origins(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "tickets_dup": {
                        "prefix": "/tickets-py",
                        "routes": [
                            {
                                "use_case": f"{module}:CreateTicketUseCase",
                                "method": "POST",
                                "path": "/",
                            }
                        ],
                    }
                }
            },
        )
        with pytest.raises(InterfaceCompilationError) as excinfo:
            create_app(config_path)
        message = str(excinfo.value)
        assert "POST" in message
        assert "/tickets-py" in message
        assert "Python interface" in message
        assert "app.rest.interfaces entry" in message

    def test_no_collision_when_methods_differ(self, tmp_path: Path) -> None:
        """Same path, different method: both origins mount without conflict."""
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "tickets_patch": {
                        "prefix": "/tickets-py",
                        "routes": [
                            {
                                "use_case": f"{module}:GetTicketUseCase",
                                "method": "PATCH",
                                "path": "/{ticket_id}",
                            }
                        ],
                    }
                }
            },
        )
        app = create_app(config_path)
        routes = _routes_under(app, "/tickets-py")
        assert ("PATCH", "/{ticket_id}") in routes
        assert ("GET", "/{ticket_id}") in routes


class TestDisableRoutesIsSubtractiveAndFailClosed:
    """``app.rest.disable_routes`` removes a route, or aborts if it names none."""

    def test_disabling_a_mounted_route_removes_only_that_route(self, tmp_path: Path) -> None:
        config_path, _module = write_project(
            tmp_path,
            rest={"disable_routes": [{"method": "GET", "path": "/tickets-py/{ticket_id}"}]},
        )
        app = create_app(config_path)
        routes = _routes_under(app, "/tickets-py")
        assert ("GET", "/{ticket_id}") not in routes
        assert ("POST", "/") in routes

    def test_disabled_route_answers_404_and_the_rest_of_the_api_still_serves(
        self, tmp_path: Path
    ) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {"prefix": "/gadgets", "auto": True, "model": f"{module}:Gadget"}
                },
                "disable_routes": [{"method": "GET", "path": "/tickets-py/{ticket_id}"}],
            },
        )
        app = create_app(config_path)
        with TestClient(app) as client:
            assert client.get("/tickets-py/1").status_code == 404
            created = client.post("/gadgets/", json={"name": "still-serving"})
            assert created.status_code == 201, created.text

    def test_an_entry_matching_no_mounted_route_aborts_startup(self, tmp_path: Path) -> None:
        config_path, _module = write_project(
            tmp_path,
            rest={"disable_routes": [{"method": "GET", "path": "/tickets-py/does-not-exist"}]},
        )
        with pytest.raises(
            InterfaceCompilationError, match="matches no route declared by a Python interface"
        ):
            create_app(config_path)

    def test_disabling_a_yaml_declared_route_is_refused(self, tmp_path: Path) -> None:
        """H4: disable_routes only ever covers Python-declared routes, never YAML ones."""
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {"prefix": "/gadgets", "auto": True, "model": f"{module}:Gadget"}
                },
                "disable_routes": [{"method": "POST", "path": "/gadgets/"}],
            },
        )
        with pytest.raises(
            InterfaceCompilationError, match="matches no route declared by a Python interface"
        ):
            create_app(config_path)

    def test_disabling_the_health_route_is_refused(self, tmp_path: Path) -> None:
        """H4: disable_routes never covers a route mounted outside interface compilation."""
        config_path, _module = write_project(
            tmp_path,
            rest={"disable_routes": [{"method": "GET", "path": "/health"}]},
        )
        with pytest.raises(
            InterfaceCompilationError, match="matches no route declared by a Python interface"
        ):
            create_app(config_path)


class TestOverridingARoutePerEnvironment:
    """H1: disable a Python route, redeclare it in YAML — the documented override flow."""

    def test_disabling_and_redeclaring_the_same_route_in_yaml_mounts_the_yaml_version(
        self, tmp_path: Path
    ) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "tickets_override": {
                        "prefix": "/tickets-py",
                        "routes": [
                            {
                                "use_case": f"{module}:GetTicketUseCase",
                                "method": "GET",
                                "path": "/{ticket_id}",
                                "status_code": 299,
                            }
                        ],
                    }
                },
                "disable_routes": [{"method": "GET", "path": "/tickets-py/{ticket_id}"}],
            },
        )
        app = create_app(config_path)

        routes = _routes_under(app, "/tickets-py")
        assert routes[("GET", "/{ticket_id}")][0] == 299


class TestUnknownKeysAbortStartup:
    """H3: a mistyped key in app.rest.interfaces / disable_routes fails closed."""

    def test_mistyped_route_key_names_the_interface_and_valid_keys(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "tickets_typo": {
                        "prefix": "/tickets-typo",
                        "routes": [
                            {
                                "use_case": f"{module}:GetTicketUseCase",
                                "method": "GET",
                                "path": "/{ticket_id}",
                                "statuscode": 299,
                            }
                        ],
                    }
                }
            },
        )
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            create_app(config_path)
        message = str(excinfo.value)
        assert "tickets_typo" in message
        assert "statuscode" in message
        assert "status_code" in message

    def test_mistyped_interface_key_names_the_interface_and_valid_keys(
        self, tmp_path: Path
    ) -> None:
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "tickets_typo": {"prefix": "/tickets-typo", "requires_role": ["admin"]}
                }
            },
        )
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            create_app(config_path)
        message = str(excinfo.value)
        assert "tickets_typo" in message
        assert "requires_role" in message
        assert "requires_roles" in message

    def test_mistyped_disable_routes_key_names_the_valid_keys(self, tmp_path: Path) -> None:
        config_path, _module = write_project(
            tmp_path,
            rest={"disable_routes": [{"method": "GET", "paths": "/tickets-py/{ticket_id}"}]},
        )
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            create_app(config_path)
        message = str(excinfo.value)
        assert "paths" in message
        assert "path" in message

    def test_a_non_mapping_interface_entry_names_the_interface_end_to_end(
        self, tmp_path: Path
    ) -> None:
        """H3: proves the guard runs on the real create_app path, not msgspec's."""
        config_path, _module = write_project(
            tmp_path,
            rest={"interfaces": {"tickets_typo": "oops"}},
        )
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            create_app(config_path)
        message = str(excinfo.value)
        assert "tickets_typo" in message
        assert "must be a mapping" in message

    def test_a_non_mapping_disable_routes_entry_names_it_end_to_end(self, tmp_path: Path) -> None:
        """H3: proves the guard runs on the real create_app path, not msgspec's."""
        config_path, _module = write_project(
            tmp_path,
            rest={"disable_routes": ["oops"]},
        )
        with pytest.raises(RestInterfaceConfigError) as excinfo:
            create_app(config_path)
        message = str(excinfo.value)
        assert "app.rest.disable_routes" in message
        assert "must be a mapping" in message


class TestRequiresRolesFromConfig:
    """H4: ``requires_roles`` declared in ``app.rest.interfaces`` is enforced."""

    def test_a_caller_without_the_declared_role_is_refused(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {
                        "prefix": "/gadgets",
                        "auto": True,
                        "model": f"{module}:Gadget",
                        "requires_roles": ["admin"],
                    }
                }
            },
        )
        app = create_app(config_path)
        token = set_identity(Identity(subject="user-1", roles=("reader",), mechanism="test"))
        try:
            response = TestClient(app).get("/gadgets/")
        finally:
            reset_identity(token)
        assert response.status_code == 403

    def test_a_caller_holding_the_declared_role_is_served(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {
                        "prefix": "/gadgets",
                        "auto": True,
                        "model": f"{module}:Gadget",
                        "requires_roles": ["admin"],
                    }
                }
            },
        )
        app = create_app(config_path)
        token = set_identity(Identity(subject="user-1", roles=("admin",), mechanism="test"))
        try:
            with TestClient(app) as client:
                response = client.get("/gadgets/")
        finally:
            reset_identity(token)
        assert response.status_code == 200

    def test_an_unauthenticated_caller_is_refused(self, tmp_path: Path) -> None:
        module = module_name(tmp_path)
        config_path, _module = write_project(
            tmp_path,
            rest={
                "interfaces": {
                    "gadgets": {
                        "prefix": "/gadgets",
                        "auto": True,
                        "model": f"{module}:Gadget",
                        "requires_roles": ["admin"],
                    }
                }
            },
        )
        app = create_app(config_path)
        assert TestClient(app).get("/gadgets/").status_code == 403
