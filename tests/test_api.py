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


def test_resize_via_api(client):
    c, _ = client
    assert c.get("/api/clones").json()["clones"] == [
        {"name": "clone-a", "block_count": 8}, {"name": "clone-b", "block_count": 8}]

    # Grow clone-a only.
    r = c.put("/api/clones/clone-a/capacity", json={"block_count": 12})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["block_count"] == 12 and body["old_block_count"] == 8
    assert c.get("/api/clones/clone-a/blocks/10").json()["source"] == "zero_unmapped"
    assert c.get("/api/clones/clone-b/blocks/10").status_code == 400

    # clone-b capacity and data unchanged.
    r = c.get("/api/clones/clone-b/blocks/3").json()
    assert r["source"] == "template"
    snap = c.get("/api/status").json()
    assert {x["name"]: x["block_count"] for x in snap["clones"]} == \
        {"clone-a": 12, "clone-b": 8}


def test_resize_shrink_refused_with_block_number(client):
    c, _ = client
    c.put("/api/clones/clone-a/blocks/6", json={"pattern": "ones"})
    r = c.put("/api/clones/clone-a/capacity", json={"block_count": 4})
    assert r.status_code == 409
    body = r.json()
    assert body["first_nonzero_block"] == 4  # first block of the tail is template data
    assert body["clone"] == "clone-a"
    # Capacity unchanged.
    assert c.get("/api/clones").json()["clones"][0]["block_count"] == 8

    # Zero the whole tail explicitly, then the shrink succeeds.
    for no in range(4, 8):
        c.put(f"/api/clones/clone-a/blocks/{no}", json={"pattern": "zero"})
    r = c.put("/api/clones/clone-a/capacity", json={"block_count": 4})
    assert r.status_code == 200
    assert c.get("/api/clones").json()["clones"][0]["block_count"] == 4
    assert c.get("/api/clones/clone-a/blocks/4").status_code == 400


def test_resize_bad_capacity_rejected(client):
    c, _ = client
    for bad in (0, 32769, -5):
        assert c.put("/api/clones/clone-a/capacity",
                     json={"block_count": bad}).status_code == 400
    assert c.put("/api/clones/ghost/capacity", json={"block_count": 4}).status_code == 400
    assert c.put("/api/clones/clone-a/capacity", json={}).status_code == 422


def test_resize_survives_app_restart(client, tmp_path):
    c, main = client
    for no in range(4, 8):
        c.put(f"/api/clones/clone-a/blocks/{no}", json={"pattern": "zero"})
    c.put("/api/clones/clone-a/capacity", json={"block_count": 4})
    c.put("/api/clones/clone-a/blocks/1", json={"pattern": "ones"})
    c.put("/api/clones/clone-b/blocks/2", json={"pattern": "ones"})

    with TestClient(main.app) as c2:
        clones = {x["name"]: x["block_count"]
                  for x in c2.get("/api/status").json()["clones"]}
        assert clones == {"clone-a": 4, "clone-b": 8}
        assert c2.get("/api/clones/clone-a/blocks/1").json()["data_hex"] == "ff" * 4096
        assert c2.get("/api/clones/clone-b/blocks/2").json()["data_hex"] == "ff" * 4096
        assert c2.get("/api/clones/clone-a/blocks/5").status_code == 400
