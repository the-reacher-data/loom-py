from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from pytest import MonkeyPatch, mark

from tests.integration.fake_repo.main import create_app_from_config


@mark.parametrize(
    "config_relpath",
    [
        "tests/integration/fake_repo/src/config/conf.modules.yaml",
        "tests/integration/fake_repo/src/config/conf.manifest.yaml",
    ],
)
def test_fake_repo_app_bootstrap_without_metrics(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    config_relpath: str,
) -> None:
    db_path = tmp_path / "fake-repo-app.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", database_url)

    config_path = Path(config_relpath)
    app = create_app_from_config(
        str(config_path),
    )

    with TestClient(app) as client:
        create_response = client.post(
            "/products/",
            json={"name": "keyboard", "price": 120.0},
        )
        assert create_response.status_code == 201
        created = create_response.json()
        assert created["id"] == 1
        assert created["name"] == "keyboard"

        list_response = client.get("/products/")
        assert list_response.status_code == 200
        listed = list_response.json()
        assert listed["total_count"] == 1
        assert listed["items"][0]["id"] == 1

        get_response = client.get("/products/1")
        assert get_response.status_code == 200
        loaded = get_response.json()
        assert loaded is not None
        assert loaded["id"] == 1
        assert loaded["name"] == "keyboard"

        update_response = client.patch("/products/1", json={"price": 99.9})
        assert update_response.status_code == 200
        updated = update_response.json()
        assert updated is not None
        assert updated["price"] == 99.9

        delete_response = client.delete("/products/1")
        assert delete_response.status_code == 200
        assert delete_response.json() is True

        missing_response = client.get("/products/1")
        assert missing_response.status_code == 200
        assert missing_response.json() is None

        metrics_response = client.get("/metrics")
        assert metrics_response.status_code == 404


def test_fake_repo_app_bootstrap_with_metrics(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    db_path = tmp_path / "fake-repo-app.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", database_url)

    config_path = Path("tests/integration/fake_repo/src/config/conf.interfaces.yaml")
    app = create_app_from_config(str(config_path))

    with TestClient(app) as client:
        create_response = client.post(
            "/products/",
            json={"name": "keyboard", "price": 120.0},
        )
        assert create_response.status_code == 201
        created = create_response.json()
        assert created["id"] == 1
        assert created["name"] == "keyboard"

        list_response = client.get("/products/")
        assert list_response.status_code == 200
        listed = list_response.json()
        assert listed["total_count"] == 1
        assert listed["items"][0]["id"] == 1

        get_response = client.get("/products/1")
        assert get_response.status_code == 200
        loaded = get_response.json()
        assert loaded is not None
        assert loaded["id"] == 1
        assert loaded["name"] == "keyboard"

        update_response = client.patch("/products/1", json={"price": 99.9})
        assert update_response.status_code == 200
        updated = update_response.json()
        assert updated is not None
        assert updated["price"] == 99.9

        delete_response = client.delete("/products/1")
        assert delete_response.status_code == 200
        assert delete_response.json() is True

        missing_response = client.get("/products/1")
        assert missing_response.status_code == 200
        assert missing_response.json() is None

        metrics_response = client.get("/metrics")
        assert metrics_response.status_code == 200
        assert "http_requests_total" in metrics_response.text
        assert "http_request_duration_seconds" in metrics_response.text


@mark.parametrize(
    "config_relpath",
    [
        "tests/integration/fake_repo/src/config/conf.modules.yaml",
        "tests/integration/fake_repo/src/config/conf.manifest.yaml",
    ],
)
def test_fake_repo_list_query_cursor_and_profile_controls(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    config_relpath: str,
) -> None:
    db_path = tmp_path / "fake-repo-query.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"
    monkeypatch.setenv("LOOM_TEST_DATABASE_URL", database_url)

    app = create_app_from_config(config_relpath)
    with TestClient(app) as client:
        assert client.post("/products/", json={"name": "a", "price": 10.0}).status_code == 201
        assert client.post("/products/", json={"name": "b", "price": 20.0}).status_code == 201
        assert client.post("/products/", json={"name": "c", "price": 20.0}).status_code == 201

        offset_response = client.get("/products/?price__eq=20&limit=1&page=1&sort=id&direction=ASC")
        assert offset_response.status_code == 200
        offset_payload = offset_response.json()
        assert offset_payload["total_count"] == 2
        assert len(offset_payload["items"]) == 1
        assert offset_payload["items"][0]["name"] == "b"

        cursor_first = client.get(
            "/products/?price__eq=20&pagination=cursor&limit=1&sort=id&direction=ASC"
        )
        assert cursor_first.status_code == 200
        cursor_first_payload = cursor_first.json()
        assert cursor_first_payload["has_next"] is True
        assert cursor_first_payload["next_cursor"] is not None
        assert cursor_first_payload["items"][0]["name"] == "b"

        next_cursor = cursor_first_payload["next_cursor"]
        cursor_second = client.get(
            f"/products/?price__eq=20&pagination=cursor&after={next_cursor}&limit=1&sort=id&direction=ASC"
        )
        assert cursor_second.status_code == 200
        cursor_second_payload = cursor_second.json()
        assert cursor_second_payload["has_next"] is False
        assert cursor_second_payload["next_cursor"] is None
        assert cursor_second_payload["items"][0]["name"] == "c"

        get_with_profile = client.get("/products/2?profile=with_details")
        assert get_with_profile.status_code == 200
        details_payload = get_with_profile.json()
        assert "hasReviews" in details_payload
        assert "countReviews" in details_payload
        assert "reviewSnippets" in details_payload

        get_invalid_profile = client.get("/products/2?profile=invalid")
        assert get_invalid_profile.status_code == 400

        post_profile_forbidden = client.post(
            "/products/?profile=with_details",
            json={"name": "forbidden", "price": 1.0},
        )
        assert post_profile_forbidden.status_code == 400
