"""HTTP-level tests against the FastAPI app using an in-process ASGI client."""
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("BLOCK_COUNT", "8")
    # Import after env vars are set; reload module so settings pick them up.
    import importlib
    import app.main as main
    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c, main


def test_health_and_status(client):
    c, _ = client
    assert c.get("/healthz").json() == {"ok": True}
    st = c.get("/api/status").json()
    assert st["block_size"] == 4096
    assert st["block_count"] == 8
    assert st["template"]["index_present"] is True
    assert {x["name"] for x in st["clones"]} == {"clone-a", "clone-b"}


def test_read_overwrite_discard_cycle(client):
    c, _ = client
    no = 3
    # initial read goes through template
    r = c.get(f"/api/clones/clone-a/blocks/{no}").json()
    assert r["source"] == "template"

    # overwrite via pattern
    resp = c.put(f"/api/clones/clone-a/blocks/{no}", json={"pattern": "ones"})
    assert resp.status_code == 200
    r = c.get(f"/api/clones/clone-a/blocks/{no}").json()
    assert r["source"] == "clone"
    assert r["data_hex"] == "ff" * 4096

    # clone-b unaffected
    assert c.get(f"/api/clones/clone-b/blocks/{no}").json()["source"] == "template"

    # discard -> zero
    assert c.delete(f"/api/clones/clone-a/blocks/{no}").status_code == 200
    r = c.get(f"/api/clones/clone-a/blocks/{no}").json()
    assert r["source"] == "zero_discard"
    assert r["data_hex"] == "00" * 4096


def test_hex_and_base64_write(client):
    c, _ = client
    data_hex = "ab" * 4096
    r = c.put("/api/clones/clone-b/blocks/0", json={"data_hex": data_hex})
    assert r.status_code == 200
    got = c.get("/api/clones/clone-b/blocks/0").json()
    assert got["data_hex"] == data_hex

    import base64
    payload = bytes(range(256)) * 16
    r = c.put("/api/clones/clone-b/blocks/1",
              json={"data_base64": base64.b64encode(payload).decode()})
    assert r.status_code == 200
    assert c.get("/api/clones/clone-b/blocks/1").json()["data_base64"] == \
        base64.b64encode(payload).decode()


def test_bad_inputs_rejected(client):
    c, _ = client
    assert c.put("/api/clones/clone-a/blocks/0", json={"data_hex": "ab"}).status_code == 400
    assert c.put("/api/clones/clone-a/blocks/0", json={}).status_code == 400
    assert c.get("/api/clones/clone-a/blocks/99").status_code == 400
    assert c.get("/api/clones/ghost/blocks/0").status_code == 400


def test_template_endpoint_then_404_after_retirement(client):
    c, main = client
    assert c.get("/api/template/blocks/0").json()["source"] == "template"
    r = c.post("/api/retirement/start")
    assert r.status_code == 200

    import time
    deadline = time.time() + 10
    while time.time() < deadline:
        st = c.get("/api/retirement").json()
        if st["state"] == "completed":
            break
        time.sleep(0.05)
    assert st["state"] == "completed"
    assert c.get("/api/template/blocks/0").status_code == 404
    snap = c.get("/api/status").json()
    assert snap["store"]["unreferenced_files"] == []
    assert snap["template"]["index_present"] is False


def test_index_page_served(client):
    c, _ = client
    r = c.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
