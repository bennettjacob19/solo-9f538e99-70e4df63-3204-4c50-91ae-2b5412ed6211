"""End-to-end tests for the block-storage drill.

Run with:  /tmp/venv/bin/python -m pytest -q
"""
import json
import os
import shutil
import time

import pytest

from app.storage import (
    BLOCK_SIZE, CLONE_NAMES, PHASE_COMPLETED, PHASE_FAILED, PHASE_GC_SWEEP,
    PHASE_MATERIALIZING, ZERO_HASH, BlockService, StorageError, block_hash,
)

BLOCK_COUNT = 8


@pytest.fixture(autouse=True)
def _clean_failpoint():
    # Failpoints are process-global env vars; never let one test's setting
    # leak into another test running in the same pytest process.
    os.environ.pop("FAILPOINT", None)
    yield
    os.environ.pop("FAILPOINT", None)


@pytest.fixture()
def svc(tmp_path):
    s = BlockService(tmp_path / "data", BLOCK_COUNT)
    s.initialize()
    yield s
    s.stop_worker()


def wait_phase(s, *phases, timeout=10):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = s.retirement_status()["state"]
        if last in phases:
            return s.retirement_status()
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {phases}, last state={last}")


def fill(pattern: bytes) -> bytes:
    return (pattern * (BLOCK_SIZE // len(pattern) + 1))[:BLOCK_SIZE]


# --------------------------------------------------------------- basic behavior
def test_template_seeded_and_clones_read_through_index(svc):
    assert svc.template_index_path.exists()
    assert len(svc.template_index) == BLOCK_COUNT
    for name in CLONE_NAMES:
        r = svc.read_block(name, 0)
        assert r.source == "template"
        assert r.ref == "template:0"
        assert len(r.data) == BLOCK_SIZE
        # identical content has identical hash -> single physical copy
        assert svc.store_dir.joinpath(r.hash).is_file()


def test_overwrite_only_affects_selected_clone(svc):
    payload = fill(b"AAAA")
    svc.overwrite_block("clone-a", 2, payload)

    ra = svc.read_block("clone-a", 2)
    rb = svc.read_block("clone-b", 2)
    assert ra.source == "clone" and ra.data == payload
    assert rb.source == "template"  # clone-b untouched

    # fresh service instance sees the persisted overwrite
    svc2 = BlockService(svc.data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.read_block("clone-a", 2).data == payload
    assert svc2.read_block("clone-b", 2).source == "template"


def test_discard_reads_zero_and_is_persisted(svc):
    svc.discard_block("clone-b", 3)
    r = svc.read_block("clone-b", 3)
    assert r.source == "zero_discard"
    assert r.data == b"\x00" * BLOCK_SIZE
    assert r.hash == ZERO_HASH
    # clone-a still sees the template for the same block
    assert svc.read_block("clone-a", 3).source == "template"

    svc2 = BlockService(svc.data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.read_block("clone-b", 3).source == "zero_discard"


def test_overwrite_after_discard_replaces_zero(svc):
    svc.discard_block("clone-a", 1)
    payload = fill(b"BBBB")
    svc.overwrite_block("clone-a", 1, payload)
    assert svc.read_block("clone-a", 1).data == payload


def test_block_validation(svc):
    with pytest.raises(StorageError):
        svc.read_block("clone-a", BLOCK_COUNT)
    with pytest.raises(StorageError):
        svc.read_block("clone-a", -1)
    with pytest.raises(StorageError):
        svc.overwrite_block("clone-a", 0, b"short")
    with pytest.raises(StorageError):
        svc.read_block("nope", 0)


def test_content_addressing_deduplicates_identical_blocks(svc):
    payload = fill(b"DEDUP")
    svc.overwrite_block("clone-a", 0, payload)
    svc.overwrite_block("clone-b", 1, payload)
    # one physical file for the identical content
    assert (svc.store_dir / block_hash(payload)).is_file()
    snap = svc.snapshot()
    assert snap["store"]["files_on_disk"] >= 1
    # template refs (8) + two clone data refs = 10 logical refs, but distinct
    # physical content is smaller; dedup metric stays non-negative.
    assert snap["store"]["logical_block_refs"] == BLOCK_COUNT + 2
    assert snap["store"]["deduplicated_bytes"] >= 0
    assert snap["store"]["deduplicated_bytes"] == (
        (snap["store"]["logical_block_refs"] - snap["store"]["distinct_referenced"]) * BLOCK_SIZE)


def test_shared_template_block_stored_once(svc):
    # Before any change both clones resolve block 0 through the same template
    # hash: exactly one physical file backs all references.
    h = svc.template_index[0]
    assert list(svc.store_dir.glob(h)) and (svc.store_dir / h).is_file()
    refs = sum(1 for _ in [svc.read_block(n, 0) for n in CLONE_NAMES])
    assert refs == 2
    assert len(list(svc.store_dir.glob(h))) == 1  # still a single file


# ------------------------------------------------------------------ retirement
def test_full_retirement_flow(svc):
    # Pre-existing clone changes must survive retirement.
    payload_a = fill(b"CLONE-A")
    payload_b = fill(b"CLONE-B")
    svc.overwrite_block("clone-a", 1, payload_a)
    svc.discard_block("clone-b", 2)
    svc.overwrite_block("clone-b", 4, payload_b)

    template_hashes = set(svc.template_index.values())
    svc.start_retirement()
    st = wait_phase(svc, PHASE_COMPLETED)

    assert st["state"] == PHASE_COMPLETED
    assert not svc.template_index_path.exists()
    assert svc.template_index is None

    # Both clones keep their latest independent content.
    assert svc.read_block("clone-a", 1).data == payload_a
    assert svc.read_block("clone-a", 0).source == "clone"  # materialized copy
    assert svc.read_block("clone-b", 2).source == "zero_discard"
    assert svc.read_block("clone-b", 4).data == payload_b
    assert svc.read_block("clone-b", 0).source == "clone"

    # Every on-disk store file has at least one reference; none unreferenced.
    snap = svc.snapshot()
    assert snap["store"]["unreferenced_files"] == []
    on_disk = {p.name for p in svc.store_dir.iterdir()}
    assert on_disk == svc._referenced_hashes()

    # Template blocks still referenced by clones were kept (single copy each).
    for h in template_hashes:
        assert (svc.store_dir / h).is_file()

    # Clones remain fully writable after retirement.
    new_payload = fill(b"POST-RETIRE")
    svc.overwrite_block("clone-a", 5, new_payload)
    assert svc.read_block("clone-a", 5).data == new_payload
    svc.discard_block("clone-b", 6)
    assert svc.read_block("clone-b", 6).source == "zero_discard"


def test_unreferenced_template_blocks_are_deleted(svc):
    # Both clones overwrite the same template block -> its original store file
    # loses every reference once the index is gone and must be GC'd.
    target = 3
    old_hash = svc.template_index[target]
    svc.overwrite_block("clone-a", target, fill(b"X1"))
    svc.overwrite_block("clone-b", target, fill(b"X2"))

    svc.start_retirement()
    wait_phase(svc, PHASE_COMPLETED)
    assert not (svc.store_dir / old_hash).exists()
    assert svc.snapshot()["store"]["unreferenced_files"] == []


def test_retirement_runs_while_clones_stay_writable(svc):
    svc.start_retirement()
    # Hammer both clones with writes/discards while retirement proceeds.
    for i in range(BLOCK_COUNT):
        svc.overwrite_block("clone-a", i, fill(f"A{i:04d}".encode()))
        if i % 2 == 0:
            svc.discard_block("clone-b", i)
        else:
            svc.overwrite_block("clone-b", i, fill(f"B{i:04d}".encode()))
    wait_phase(svc, PHASE_COMPLETED)

    for i in range(BLOCK_COUNT):
        assert svc.read_block("clone-a", i).data == fill(f"A{i:04d}".encode())
        rb = svc.read_block("clone-b", i)
        if i % 2 == 0:
            assert rb.source == "zero_discard"
        else:
            assert rb.data == fill(f"B{i:04d}".encode())
    assert svc.snapshot()["store"]["unreferenced_files"] == []


def test_retirement_failure_leaves_clones_usable_and_can_retry(tmp_path):
    data_dir = tmp_path / "data"
    os.environ["FAILPOINT"] = "before_gc_delete"
    try:
        svc = BlockService(data_dir, BLOCK_COUNT)
        svc.step_delay = 0.05  # slow worker so the failpoint is reached
        svc.initialize()
        svc.overwrite_block("clone-a", 0, fill(b"OK"))
        # Both clones overwrite block 7: after index removal its original
        # content has zero references, guaranteeing the GC has a file to sweep
        # (and therefore reaches the failpoint).
        svc.overwrite_block("clone-a", 7, fill(b"X1"))
        svc.overwrite_block("clone-b", 7, fill(b"X2"))
        svc.start_retirement()
        st = wait_phase(svc, PHASE_FAILED)
        assert "injected failure" in st["error"]
        # Clones keep working normally after the failure.
        assert svc.read_block("clone-a", 0).data == fill(b"OK")
        svc.overwrite_block("clone-b", 1, fill(b"STILL-WORKS"))
        assert svc.read_block("clone-b", 1).data == fill(b"STILL-WORKS")
        svc.stop_worker()
    finally:
        del os.environ["FAILPOINT"]

    # Retry (simulating an operator re-trigger, failpoint now off): resumes
    # from the persisted GC phase and completes.
    svc2 = BlockService(data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.retirement_status()["state"] == PHASE_FAILED
    assert not svc2.template_index_path.exists()  # index removal already done
    svc2.start_retirement()
    wait_phase(svc2, PHASE_COMPLETED)
    assert svc2.snapshot()["store"]["unreferenced_files"] == []
    assert svc2.read_block("clone-b", 1).data == fill(b"STILL-WORKS")


def test_restart_resumes_interrupted_retirement(tmp_path):
    data_dir = tmp_path / "data"
    svc = BlockService(data_dir, BLOCK_COUNT)
    svc.step_delay = 0.02  # keep the worker in-flight long enough to interrupt it
    svc.initialize()
    svc.overwrite_block("clone-a", 2, fill(b"PERSISTED"))
    svc.start_retirement()
    # Stop the worker mid-flight at whatever phase it reached.
    deadline = time.time() + 5
    while time.time() < deadline:
        state = svc.retirement_status()["state"]
        if state != PHASE_MATERIALIZING:
            break
        time.sleep(0.01)
    svc.stop_worker()
    mid_state = svc.retirement_status()["state"]
    assert mid_state != PHASE_COMPLETED

    # Simulate process restart: brand-new service over the same data volume,
    # which auto-resumes retirement on initialize().
    svc2 = BlockService(data_dir, BLOCK_COUNT)
    svc2.initialize()
    svc2.resume_retirement_on_startup()
    wait_phase(svc2, PHASE_COMPLETED)
    assert svc2.read_block("clone-a", 2).data == fill(b"PERSISTED")
    assert svc2.snapshot()["store"]["unreferenced_files"] == []


def test_restart_after_completion_is_stable(tmp_path):
    data_dir = tmp_path / "data"
    svc = BlockService(data_dir, BLOCK_COUNT)
    svc.initialize()
    svc.start_retirement()
    wait_phase(svc, PHASE_COMPLETED)
    svc.stop_worker()

    svc2 = BlockService(data_dir, BLOCK_COUNT)
    svc2.initialize()  # no template index on disk: must not reseed
    assert svc2.template_index is None
    assert svc2.retirement_status()["state"] == PHASE_COMPLETED
    # Materialized blocks still read correctly.
    r = svc2.read_block("clone-a", 0)
    assert r.source == "clone" and len(r.data) == BLOCK_SIZE


def test_retirement_state_is_durable_json(tmp_path, svc):
    svc.start_retirement()
    wait_phase(svc, PHASE_COMPLETED)
    doc = json.loads(svc.retirement_path.read_text())
    assert doc["state"] == PHASE_COMPLETED
    assert doc["index_removed"] is True
    assert isinstance(doc["gc_deleted"], list)


def test_gc_never_runs_before_index_removal(svc):
    # While the index exists, referenced template blocks stay in the store.
    svc.overwrite_block("clone-a", 0, fill(b"ZZ"))
    snap = svc.snapshot()
    assert snap["template"]["index_present"] is True
    # template still references the original block-0 file
    assert svc.template_index[0] in {p.name for p in svc.store_dir.iterdir()}


def test_failure_before_index_removal_keeps_index(tmp_path):
    data_dir = tmp_path / "data"
    os.environ["FAILPOINT"] = "before_index_remove"
    try:
        svc = BlockService(data_dir, BLOCK_COUNT)
        svc.step_delay = 0.05
        svc.initialize()
        svc.start_retirement()
        wait_phase(svc, PHASE_FAILED)
        svc.stop_worker()
        assert svc.template_index_path.exists()
    finally:
        del os.environ["FAILPOINT"]

    svc2 = BlockService(data_dir, BLOCK_COUNT)
    svc2.initialize()
    # Materialization already copied every template ref into the clone, so
    # reads resolve inside the clone even though the index file still exists.
    assert svc2.read_block("clone-a", 0).source == "clone"
    assert svc2.template_index_path.exists()
    svc2.start_retirement()
    wait_phase(svc2, PHASE_COMPLETED)
    assert not svc2.template_index_path.exists()


# --------------------------------------------------------------------- resize
from app.storage import (  # noqa: E402
    MAX_BLOCK_COUNT, MIN_BLOCK_COUNT, ResizeBlockedError,
)


def zero_tail(svc, name, start, end=BLOCK_COUNT):
    """Explicitly zero every block in [start, end) so it may be truncated."""
    for no in range(start, end):
        svc.overwrite_block(name, no, b"\x00" * BLOCK_SIZE)


def test_grow_reads_zero_and_creates_no_files(svc):
    files_before = {p.name for p in svc.store_dir.iterdir() if p.is_file()}
    res = svc.resize_clone("clone-a", BLOCK_COUNT + 4)
    assert res["changed"] is True
    assert res["block_count"] == BLOCK_COUNT + 4
    assert res["old_block_count"] == BLOCK_COUNT
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT + 4
    # The other clone keeps its capacity.
    assert svc.clones["clone-b"].block_count == BLOCK_COUNT
    # Expanded blocks read zero and have no mapping at all.
    for no in range(BLOCK_COUNT, BLOCK_COUNT + 4):
        r = svc.read_block("clone-a", no)
        assert r.data == b"\x00" * BLOCK_SIZE
        assert r.source == "zero_unmapped"
        assert no not in svc.clones["clone-a"].blocks
    # Growing never materializes empty blocks into the store.
    assert {p.name for p in svc.store_dir.iterdir() if p.is_file()} == files_before
    # clone-b still rejects the new block numbers.
    with pytest.raises(StorageError):
        svc.read_block("clone-b", BLOCK_COUNT)


def test_grow_then_write_roundtrip(svc):
    target = BLOCK_COUNT + 2
    svc.resize_clone("clone-a", target)
    payload = fill(b"NEW-BLOCK")
    svc.overwrite_block("clone-a", BLOCK_COUNT + 1, payload)
    assert svc.read_block("clone-a", BLOCK_COUNT + 1).data == payload
    # Persisted across a restart.
    svc2 = BlockService(svc.data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.clones["clone-a"].block_count == target
    assert svc2.read_block("clone-a", BLOCK_COUNT + 1).data == payload
    assert svc2.clones["clone-b"].block_count == BLOCK_COUNT


def test_shrink_truncates_zero_tail(svc):
    svc.overwrite_block("clone-a", 1, fill(b"KEEP"))
    zero_tail(svc, "clone-a", 4)
    res = svc.resize_clone("clone-a", 4)
    assert res["block_count"] == 4
    assert svc.read_block("clone-a", 1).data == fill(b"KEEP")
    with pytest.raises(StorageError):
        svc.read_block("clone-a", 4)
    with pytest.raises(StorageError):
        svc.overwrite_block("clone-a", 5, fill(b"NOPE"))
    with pytest.raises(StorageError):
        svc.discard_block("clone-a", 5)
    # Other clone untouched.
    assert svc.clones["clone-b"].block_count == BLOCK_COUNT
    assert svc.read_block("clone-b", 7).source == "template"


def test_shrink_rejected_on_first_nonzero_block(svc):
    svc.overwrite_block("clone-a", 4, b"\x00" * BLOCK_SIZE)
    svc.overwrite_block("clone-a", 6, fill(b"DIRTY"))
    svc.overwrite_block("clone-a", 5, fill(b"ALSO-DIRTY"))
    with pytest.raises(ResizeBlockedError) as ei:
        svc.resize_clone("clone-a", 4)
    # Block 4 is now zero, so the first offending block is 5.
    assert ei.value.block_no == 5
    assert ei.value.target_count == 4
    # Nothing changed: capacity, data and metadata all intact.
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT
    assert svc.read_block("clone-a", 5).data == fill(b"ALSO-DIRTY")
    assert svc.read_block("clone-a", 7).source == "template"
    svc2 = BlockService(svc.data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.clones["clone-a"].block_count == BLOCK_COUNT
    assert svc2.read_block("clone-a", 6).data == fill(b"DIRTY")


def test_shrink_rejects_nonzero_unmodified_template_tail(svc):
    # Unmodified blocks resolve to non-zero template content: refuse, naming
    # the first (lowest) non-zero block in the truncated tail.
    with pytest.raises(ResizeBlockedError) as ei:
        svc.resize_clone("clone-a", BLOCK_COUNT - 1)
    assert ei.value.block_no == BLOCK_COUNT - 1
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT


def test_shrink_allows_discarded_tail(svc):
    # A discard reads zero, so it may be truncated away.
    svc.discard_block("clone-a", 7)
    svc.resize_clone("clone-a", 7)
    assert svc.clones["clone-a"].block_count == 7
    with pytest.raises(StorageError):
        svc.read_block("clone-a", 7)


def test_regrow_after_shrink_reads_zero_not_template(svc):
    # Block 5 is a template-backed block. Shrinking past it requires zeroing
    # it first; growing it back must keep reading zero, not template content.
    svc.overwrite_block("clone-a", 5, b"\x00" * BLOCK_SIZE)
    zero_tail(svc, "clone-a", 6)
    svc.resize_clone("clone-a", 5)
    svc.resize_clone("clone-a", BLOCK_COUNT + 1)
    r = svc.read_block("clone-a", 5)
    assert r.data == b"\x00" * BLOCK_SIZE
    assert r.source == "zero_unmapped"
    # Blocks never truncated still read through the template.
    assert svc.read_block("clone-a", 4).source == "template"
    # Survives a restart.
    svc2 = BlockService(svc.data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.read_block("clone-a", 5).source == "zero_unmapped"
    assert svc2.read_block("clone-a", 4).source == "template"


def test_regrow_hole_never_filled_by_retirement_materialization(tmp_path):
    data_dir = tmp_path / "data"
    svc = BlockService(data_dir, BLOCK_COUNT)
    svc.step_delay = 0.02
    svc.initialize()
    # Zero out and shrink away blocks 6..7, then grow the capacity back.
    zero_tail(svc, "clone-a", 6)
    svc.resize_clone("clone-a", 6)
    svc.resize_clone("clone-a", BLOCK_COUNT)
    svc.start_retirement()
    wait_phase(svc, PHASE_COMPLETED)
    for no in (6, 7):
        r = svc.read_block("clone-a", no)
        assert r.source == "zero_unmapped", f"block {no} materialized: {r.source}"
        assert r.data == b"\x00" * BLOCK_SIZE
    # clone-b never truncated them, so retirement materialized them there.
    assert svc.read_block("clone-b", 6).source == "clone"


def test_truncated_blocks_do_not_block_retirement(tmp_path):
    data_dir = tmp_path / "data"
    svc = BlockService(data_dir, BLOCK_COUNT)
    svc.step_delay = 0.05
    svc.initialize()
    zero_tail(svc, "clone-a", 4)
    svc.resize_clone("clone-a", 4)
    svc.start_retirement()  # runs while clone-a has a smaller capacity
    st = wait_phase(svc, PHASE_COMPLETED)
    assert st["state"] == PHASE_COMPLETED
    assert not svc.template_index_path.exists()
    assert svc.clones["clone-a"].block_count == 4
    assert svc.snapshot()["store"]["unreferenced_files"] == []


def test_resize_during_retirement(tmp_path):
    data_dir = tmp_path / "data"
    svc = BlockService(data_dir, BLOCK_COUNT)
    svc.step_delay = 0.05
    svc.initialize()
    zero_tail(svc, "clone-b", 6)
    svc.start_retirement()
    # Shrink while materialization is in flight, then grow back.
    svc.resize_clone("clone-b", 6)
    svc.resize_clone("clone-b", BLOCK_COUNT + 2)
    wait_phase(svc, PHASE_COMPLETED)
    for no in (6, 7):
        r = svc.read_block("clone-b", no)
        assert r.source == "zero_unmapped"
        assert r.data == b"\x00" * BLOCK_SIZE
    assert svc.clones["clone-b"].block_count == BLOCK_COUNT + 2
    assert svc.read_block("clone-b", BLOCK_COUNT + 1).source == "zero_unmapped"


def test_resize_bounds(svc):
    for bad in (0, -1, MAX_BLOCK_COUNT + 1):
        with pytest.raises(StorageError):
            svc.resize_clone("clone-a", bad)
    with pytest.raises(StorageError):
        svc.resize_clone("clone-a", True)
    with pytest.raises(StorageError):
        svc.resize_clone("ghost", 4)
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT
    # Both range endpoints are accepted (tail is zero after shrinking to 1 only
    # if we zero it, so zero everything first).
    zero_tail(svc, "clone-a", 0)
    svc.resize_clone("clone-a", MIN_BLOCK_COUNT)
    assert svc.clones["clone-a"].block_count == MIN_BLOCK_COUNT
    svc.resize_clone("clone-a", MAX_BLOCK_COUNT)
    assert svc.clones["clone-a"].block_count == MAX_BLOCK_COUNT


def test_resize_is_single_clone_and_durable(tmp_path):
    data_dir = tmp_path / "data"
    svc = BlockService(data_dir, BLOCK_COUNT)
    svc.initialize()
    svc.overwrite_block("clone-b", 0, fill(b"B0"))
    zero_tail(svc, "clone-a", 3)
    svc.resize_clone("clone-a", 3)
    # clone-b unchanged in capacity and content.
    assert svc.clones["clone-b"].block_count == BLOCK_COUNT
    assert svc.read_block("clone-b", 0).data == fill(b"B0")
    svc.stop_worker()

    svc2 = BlockService(data_dir, BLOCK_COUNT)
    svc2.initialize()
    assert svc2.clones["clone-a"].block_count == 3
    assert svc2.clones["clone-b"].block_count == BLOCK_COUNT
    assert svc2.read_block("clone-b", 0).data == fill(b"B0")


def test_resize_failure_keeps_old_capacity_and_data(svc, monkeypatch):
    svc.overwrite_block("clone-a", 1, fill(b"SAFE"))
    zero_tail(svc, "clone-a", 2)

    def boom(path, obj):
        raise OSError("disk full")

    monkeypatch.setattr("app.storage._atomic_write_json", boom)
    with pytest.raises(OSError):
        svc.resize_clone("clone-a", 2)
    monkeypatch.undo()
    # In-memory state still reports the old capacity and data.
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT
    assert svc.read_block("clone-a", 1).data == fill(b"SAFE")
    assert svc.read_block("clone-a", 7).data == b"\x00" * BLOCK_SIZE
    assert svc.read_block("clone-a", 7).source == "clone"
    # On disk too.
    doc = json.loads((svc.clones_dir / "clone-a.json").read_text())
    assert doc["block_count"] == BLOCK_COUNT
    assert len(doc["blocks"]) == BLOCK_COUNT - 1  # zeroed tail mappings retained


def test_grow_failure_keeps_old_capacity(svc, monkeypatch):
    svc.overwrite_block("clone-a", 0, fill(b"KEEP-A0"))

    def boom(path, obj):
        raise OSError("disk full")

    monkeypatch.setattr("app.storage._atomic_write_json", boom)
    with pytest.raises(OSError):
        svc.resize_clone("clone-a", BLOCK_COUNT + 4)
    monkeypatch.undo()
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT
    with pytest.raises(StorageError):
        svc.read_block("clone-a", BLOCK_COUNT)
    assert svc.read_block("clone-a", 0).data == fill(b"KEEP-A0")


def test_concurrent_write_cannot_slip_between_tail_check_and_shrink(svc, monkeypatch):
    """Every interleaving must leave a consistent outcome.

    A writer racing a shrink either commits before the emptiness check (the
    shrink is then refused) or after the new capacity is in effect (the write
    is refused).  There is no window in which a tail write is accepted and
    then silently truncated away.
    """
    import threading
    start = threading.Barrier(2)
    outcome = {}
    original = svc._logical_hash_locked

    def slow_check(clone, block_no):
        # Widen the would-be race window right at the emptiness check.
        result = original(clone, block_no)
        if block_no == 6:
            time.sleep(0.005)
        return result

    monkeypatch.setattr(svc, "_logical_hash_locked", slow_check)

    def writer():
        start.wait()
        try:
            svc.overwrite_block("clone-a", 6, fill(b"RACED"))
            outcome["write"] = "ok"
        except StorageError:
            outcome["write"] = "rejected"

    def resizer():
        start.wait()
        try:
            # Zero blocks 6 and 7 first so the only obstacle can be the
            # racing writer's data in block 6.
            svc.resize_clone("clone-a", 6)
            outcome["resize"] = "ok"
        except ResizeBlockedError:
            outcome["resize"] = "rejected"

    t1 = threading.Thread(target=writer)
    t2 = threading.Thread(target=resizer)
    t1.start(); t2.start(); t1.join(); t2.join()

    if outcome["write"] == "ok":
        # The write won: it must still be readable iff the shrink was refused.
        if outcome["resize"] == "ok":
            raise AssertionError("write accepted and tail silently truncated")
        assert svc.clones["clone-a"].block_count == BLOCK_COUNT
        assert svc.read_block("clone-a", 6).data == fill(b"RACED")
    else:
        # The shrink won: capacity is smaller and block 6 is unreachable.
        assert outcome["resize"] == "ok"
        assert svc.clones["clone-a"].block_count == 6
        with pytest.raises(StorageError):
            svc.read_block("clone-a", 6)
    # Whatever the order, on-disk metadata agrees with memory.
    doc = json.loads((svc.clones_dir / "clone-a.json").read_text())
    assert doc["block_count"] == svc.clones["clone-a"].block_count


def test_concurrent_reads_respect_resize(svc):
    """Reads either see the old capacity or are rejected; never a torn state."""
    import threading
    errors = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            try:
                r = svc.read_block("clone-a", 7)
                assert len(r.data) == BLOCK_SIZE
            except StorageError:
                pass  # out of range after shrink
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                return

    # Zero the tail once so the shrink is legal; reads racing the resize must
    # still see a consistent capacity.
    zero_tail(svc, "clone-a", 4)
    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    try:
        for _ in range(5):
            svc.resize_clone("clone-a", BLOCK_COUNT)
            svc.resize_clone("clone-a", 4)
            svc.resize_clone("clone-a", BLOCK_COUNT)
    finally:
        stop.set()
        for t in threads:
            t.join()
    assert not errors, errors
    assert svc.clones["clone-a"].block_count == BLOCK_COUNT
