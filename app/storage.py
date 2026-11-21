"""Local block storage with a read-only template and two COW clones.

On-disk layout (``DATA_DIR``, mounted as a Docker volume)::

    template/index.json        # block_no -> sha256, immutable until retirement
    store/<sha256>             # content-addressed 4 KiB blocks (dedup store)
    clones/<name>.json         # clone metadata (overwrites + discards)
    retirement.json            # durable retirement state machine

Every mutation is persisted with a temp-file + fsync + atomic rename before the
API call returns, so a process crash can never lose a confirmed overwrite or
discard, and an interrupted retirement simply resumes on the next startup.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

BLOCK_SIZE = 4096
ZEROS = b"\x00" * BLOCK_SIZE
ZERO_HASH = hashlib.sha256(ZEROS).hexdigest()
CLONE_NAMES = ("clone-a", "clone-b")

# Retirement state machine phases.
PHASE_MATERIALIZING = "materializing"   # copy template refs into each clone
PHASE_INDEX_REMOVED = "index_removed"   # template index file deleted
PHASE_GC_SWEEP = "gc_sweep"             # delete unreferenced store blocks
PHASE_COMPLETED = "completed"
PHASE_FAILED = "failed"

STORE_DIRNAME = "store"
TEMPLATE_DIRNAME = "template"
CLONES_DIRNAME = "clones"
RET_FILENAME = "retirement.json"
INDEX_FILENAME = "index.json"


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes durably: temp file + fsync + atomic rename + dir fsync."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp-" + str(os.getpid()) + "-" + str(time.time_ns()))
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_json(path: Path, obj: Any) -> None:
    _atomic_write_bytes(path, json.dumps(obj, indent=2, sort_keys=True).encode("utf-8"))


def block_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass
class BlockRead:
    data: bytes
    source: str  # "clone" | "zero_discard" | "template" | "zero_unmapped"
    hash: str
    ref: Optional[str]  # "template:<no>" | "clone:<no>" | None
    materialized_from_template: bool = False


class StorageError(Exception):
    pass


class Clone:
    """A COW clone. Metadata layout (version 1)::

        {"name": ..., "block_count": N,
         "blocks": {"<no>": {"kind": "data"|"zero", "hash": ...}}}
    """

    def __init__(self, path: Path, name: str, block_count: int):
        self.path = path
        self.name = name
        self.block_count = block_count
        self.lock = threading.RLock()
        self.blocks: dict[int, dict[str, Any]] = {}
        if path.exists():
            doc = json.loads(path.read_text("utf-8"))
            self.block_count = doc["block_count"]
            self.blocks = {int(k): v for k, v in doc["blocks"].items()}

    def _persist(self) -> None:
        doc = {
            "version": 1,
            "name": self.name,
            "block_count": self.block_count,
            "blocks": {str(k): v for k, v in sorted(self.blocks.items())},
        }
        _atomic_write_json(self.path, doc)

    def validate_block_no(self, block_no: int) -> None:
        if not isinstance(block_no, int) or block_no < 0 or block_no >= self.block_count:
            raise StorageError(
                f"block_no must be an integer in [0, {self.block_count - 1}], got {block_no!r}"
            )


class BlockService:
    """Owns the on-disk volume, template, clones and the retirement worker."""

    def __init__(self, data_dir: str | os.PathLike[str], block_count: int,
                 seed: str = "template-disk-v1"):
        self.data_dir = Path(data_dir)
        self.block_count = block_count
        self.seed = seed
        self.store_dir = self.data_dir / STORE_DIRNAME
        self.template_dir = self.data_dir / TEMPLATE_DIRNAME
        self.clones_dir = self.data_dir / CLONES_DIRNAME
        self.template_index_path = self.template_dir / INDEX_FILENAME
        self.retirement_path = self.data_dir / RET_FILENAME

        # Global lock: held by clone writes while touching metadata/store and
        # by the GC sweep; guarantees a block is never garbage-collected while
        # a brand-new reference to it is being created.
        self.global_lock = threading.RLock()
        self._ret_lock = threading.RLock()
        self._worker: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()
        # Per-step pause inside the retirement worker. Zero by default; tests
        # (and slow-motion drills) set this to make the background phases
        # observable and failure injection deterministic.
        self.step_delay = float(os.environ.get("RETIRE_STEP_DELAY", "0"))

        self.template_index: Optional[dict[int, str]] = None
        self.clones: dict[str, Clone] = {}
        self.retirement: dict[str, Any] = {}

    # ------------------------------------------------------------------ setup
    def initialize(self) -> None:
        """Create volume structure, seed the template, load persisted state."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.store_dir.mkdir(exist_ok=True)
        self.template_dir.mkdir(exist_ok=True)
        self.clones_dir.mkdir(exist_ok=True)

        if self.template_index_path.exists():
            doc = json.loads(self.template_index_path.read_text("utf-8"))
            self.template_index = {int(k): v for k, v in doc["blocks"].items()}
            self.block_count = doc["block_count"]
        elif self.retirement_path.exists():
            # Retirement was started in a previous process (the index may be
            # already removed, or removal may have failed before unlink): never
            # reseed a template that is being or has been retired.
            prior = json.loads(self.retirement_path.read_text("utf-8"))
            if prior.get("state") not in ("idle", None):
                self.template_index = None
            else:
                self._seed_template()
        else:
            self._seed_template()

        for name in CLONE_NAMES:
            path = self.clones_dir / f"{name}.json"
            self.clones[name] = Clone(path, name, self.block_count)

        if self.retirement_path.exists():
            self.retirement = json.loads(self.retirement_path.read_text("utf-8"))
        else:
            self.retirement = {"state": "idle"}

    def _seed_template(self) -> None:
        """Create the read-only template: N deterministic 4 KiB blocks."""
        index: dict[int, str] = {}
        for no in range(self.block_count):
            data = self._template_block(no)
            h = block_hash(data)
            index[no] = h
            store_path = self.store_dir / h
            if not store_path.exists():
                _atomic_write_bytes(store_path, data)
        _atomic_write_json(self.template_index_path, {
            "version": 1,
            "seed": self.seed,
            "block_size": BLOCK_SIZE,
            "block_count": self.block_count,
            "blocks": {str(k): v for k, v in sorted(index.items())},
        })
        self.template_index = index

    def _template_block(self, no: int) -> bytes:
        header = f"TEMPLATE|seed={self.seed}|block={no}|".encode("utf-8")
        pattern = bytes(((no * 37 + i * 7) & 0xFF) for i in range(BLOCK_SIZE))
        return (header + pattern)[:BLOCK_SIZE].ljust(BLOCK_SIZE, b"\x00")

    # ------------------------------------------------------------------ helpers
    def _store_put(self, data: bytes) -> str:
        h = block_hash(data)
        path = self.store_dir / h
        if not path.exists():
            _atomic_write_bytes(path, data)
        return h

    def _store_path(self, h: str) -> Path:
        return self.store_dir / h

    def _persist_retirement(self) -> None:
        _atomic_write_json(self.retirement_path, self.retirement)

    def _set_ret_failed(self, reason: str) -> None:
        self.retirement = {
            **self.retirement,
            "state": PHASE_FAILED,
            "error": reason,
            "updated_at": time.time(),
        }
        self._persist_retirement()

    # ------------------------------------------------------------------ reads
    def read_block(self, clone_name: str, block_no: int) -> BlockRead:
        clone = self._get_clone(clone_name)
        clone.validate_block_no(block_no)
        with clone.lock:
            entry = clone.blocks.get(block_no)
            if entry is not None:
                if entry["kind"] == "zero":
                    return BlockRead(ZEROS, "zero_discard", ZERO_HASH,
                                     f"clone:{block_no}")
                h = entry["hash"]
                data = self._store_path(h).read_bytes()
                return BlockRead(data, "clone", h, f"clone:{block_no}")

            # Unmapped: fall back to the template index while it still exists.
            if self.template_index is not None and block_no in self.template_index:
                h = self.template_index[block_no]
                data = self._store_path(h).read_bytes()
                return BlockRead(data, "template", h, f"template:{block_no}")
            return BlockRead(ZEROS, "zero_unmapped", ZERO_HASH, None)

    # ------------------------------------------------------------------ writes
    def _release_if_orphaned(self, h: str) -> bool:
        """Delete a store file once no clone/template references its hash.

        Must be called under ``global_lock`` so the reference scan and the
        unlink are atomic relative to every other writer and the GC sweep.
        """
        if h == ZERO_HASH:
            return False
        if h in self._referenced_hashes():
            return False
        path = self._store_path(h)
        if path.exists():
            os.remove(path)
            _fsync_dir(self.store_dir)
            return True
        return False

    def overwrite_block(self, clone_name: str, block_no: int, data: bytes) -> dict[str, Any]:
        if len(data) != BLOCK_SIZE:
            raise StorageError(f"block data must be exactly {BLOCK_SIZE} bytes, got {len(data)}")
        clone = self._get_clone(clone_name)
        clone.validate_block_no(block_no)
        with self.global_lock:
            with clone.lock:
                old = clone.blocks.get(block_no)
                h = self._store_put(data)
                clone.blocks[block_no] = {"kind": "data", "hash": h}
                clone._persist()
                # Reclaim the replaced physical block immediately if nothing
                # else references it, so the volume stays GC-clean at rest.
                if old is not None and old["kind"] == "data" and old["hash"] != h:
                    self._release_if_orphaned(old["hash"])
        return {"block_no": block_no, "hash": h, "kind": "data"}

    def discard_block(self, clone_name: str, block_no: int) -> dict[str, Any]:
        clone = self._get_clone(clone_name)
        clone.validate_block_no(block_no)
        with self.global_lock:
            with clone.lock:
                old = clone.blocks.get(block_no)
                clone.blocks[block_no] = {"kind": "zero", "hash": ZERO_HASH}
                clone._persist()
                if old is not None and old["kind"] == "data":
                    self._release_if_orphaned(old["hash"])
        return {"block_no": block_no, "hash": ZERO_HASH, "kind": "zero"}

    def _get_clone(self, name: str) -> Clone:
        try:
            return self.clones[name]
        except KeyError:
            raise StorageError(f"unknown clone {name!r}; expected one of {CLONE_NAMES}")

    # ------------------------------------------------------------- introspection
    def snapshot(self) -> dict[str, Any]:
        referenced = self._referenced_hashes()
        on_disk = {p.name for p in self.store_dir.iterdir() if p.is_file()}
        unreferenced = sorted(on_disk - referenced)
        template_blocks = len(self.template_index) if self.template_index is not None else 0

        # Logical block references vs distinct physical content: the gap is
        # what content-addressed dedup saves (shared template / COW blocks).
        logical_refs = 0
        if self.template_index is not None:
            logical_refs += len(self.template_index)
        for clone in self.clones.values():
            with clone.lock:
                logical_refs += sum(1 for e in clone.blocks.values() if e["kind"] == "data")
        distinct = len(referenced)
        return {
            "block_size": BLOCK_SIZE,
            "block_count": self.block_count,
            "template": {
                "index_present": self.template_index is not None,
                "index_path": str(self.template_index_path),
                "blocks": template_blocks,
            },
            "clones": [self._clone_snapshot(name) for name in CLONE_NAMES],
            "store": {
                "directory": str(self.store_dir),
                "files_on_disk": len(on_disk),
                "logical_block_refs": logical_refs,
                "distinct_referenced": distinct,
                "unreferenced_files": unreferenced,
                "deduplicated_bytes": max(0, logical_refs - distinct) * BLOCK_SIZE,
            },
            "retirement": self.retirement_status(),
        }

    def _clone_snapshot(self, name: str) -> dict[str, Any]:
        clone = self.clones[name]
        with clone.lock:
            overwrites = sorted(no for no, e in clone.blocks.items() if e["kind"] == "data")
            discards = sorted(no for no, e in clone.blocks.items() if e["kind"] == "zero")
        return {
            "name": name,
            "block_count": clone.block_count,
            "overwrites": overwrites,
            "discards": discards,
            "changed_blocks": len(clone.blocks),
        }

    def _referenced_hashes(self) -> set[str]:
        refs: set[str] = set()
        if self.template_index is not None:
            refs.update(self.template_index.values())
        for clone in self.clones.values():
            with clone.lock:
                for entry in clone.blocks.values():
                    if entry["kind"] == "data":
                        refs.add(entry["hash"])
        return refs

    def read_template_block(self, block_no: int) -> Optional[BlockRead]:
        if not isinstance(block_no, int) or block_no < 0 or block_no >= self.block_count:
            raise StorageError(
                f"block_no must be an integer in [0, {self.block_count - 1}], got {block_no!r}")
        if self.template_index is None or block_no not in self.template_index:
            return None
        h = self.template_index[block_no]
        return BlockRead(self._store_path(h).read_bytes(), "template", h,
                         f"template:{block_no}")

    # =============================================================== retirement
    def retirement_status(self) -> dict[str, Any]:
        with self._ret_lock:
            return dict(self.retirement)

    def start_retirement(self) -> dict[str, Any]:
        """Kick off (or resume) background template retirement."""
        with self._ret_lock:
            state = self.retirement.get("state", "idle")
            if state in (PHASE_MATERIALIZING, PHASE_INDEX_REMOVED, PHASE_GC_SWEEP):
                return {"started": False, "status": dict(self.retirement),
                        "message": "retirement already running"}
            if state == PHASE_COMPLETED:
                return {"started": False, "status": dict(self.retirement),
                        "message": "retirement already completed"}
            if state == PHASE_FAILED:
                # Allow retrying after a failure: resume from the persisted phase.
                self.retirement["state"] = self.retirement.get("resume_phase",
                                                               PHASE_MATERIALIZING)
                self.retirement.pop("error", None)
                self._persist_retirement()
            else:
                self.retirement = {
                    "state": PHASE_MATERIALIZING,
                    "started_at": time.time(),
                    "phase": PHASE_MATERIALIZING,
                    "materialize_cursor": 0,
                    "clones_materialized": [],
                    "index_removed": False,
                    "gc_deleted": [],
                    "resume_phase": PHASE_MATERIALIZING,
                }
                self._persist_retirement()

        self._stop_flag.clear()
        self._worker = threading.Thread(
            target=self._run_retirement, name="template-retirement", daemon=True)
        self._worker.start()
        return {"started": True, "status": self.retirement_status()}

    def resume_retirement_on_startup(self) -> None:
        """Continue an interrupted retirement after a process restart."""
        state = self.retirement.get("state", "idle")
        if state in (PHASE_MATERIALIZING, PHASE_INDEX_REMOVED, PHASE_GC_SWEEP):
            self._stop_flag.clear()
            self._worker = threading.Thread(
                target=self._run_retirement, name="template-retirement", daemon=True)
            self._worker.start()

    def stop_worker(self) -> None:
        self._stop_flag.set()

    def _run_retirement(self) -> None:
        try:
            self._retire_materialize()
            self._retire_remove_index()
            self._retire_gc()
            with self._ret_lock:
                self.retirement.update({
                    "state": PHASE_COMPLETED,
                    "phase": PHASE_COMPLETED,
                    "completed_at": time.time(),
                    "updated_at": time.time(),
                })
                self._persist_retirement()
        except Exception as exc:  # surface failures via API; clones keep working
            with self._ret_lock:
                self._set_ret_failed(f"{type(exc).__name__}: {exc}")

    # ---- phase 1: give every clone its own reference to every template block
    def _retire_materialize(self) -> None:
        batch = max(1, int(os.environ.get("RETIRE_BATCH_SIZE", "8")))
        while True:
            with self._ret_lock:
                if self.retirement.get("state") != PHASE_MATERIALIZING:
                    return
                done = list(self.retirement.get("clones_materialized", []))
                cursor = int(self.retirement.get("materialize_cursor", 0))

            for name in CLONE_NAMES:
                if name in done:
                    continue
                clone = self.clones[name]
                if self.template_index is None:
                    # Index vanished (e.g. completed in a prior run): mark done.
                    with self._ret_lock:
                        done = list(self.retirement.get("clones_materialized", []))
                        if name not in done:
                            done.append(name)
                        self.retirement["clones_materialized"] = done
                        self._persist_retirement()
                    continue
                template_items = sorted(self.template_index.items())
                while cursor < len(template_items):
                    if self._stop_flag.is_set():
                        return
                    # Brief lock per batch: clone I/O stays available between
                    # batches; clone-local entries always win over materialization.
                    with self.global_lock:
                        with clone.lock:
                            for no, h in template_items[cursor:cursor + batch]:
                                if no not in clone.blocks:
                                    clone.blocks[no] = {"kind": "data", "hash": h}
                            clone._persist()
                    cursor = min(cursor + batch, len(template_items))
                    with self._ret_lock:
                        self.retirement["materialize_cursor"] = cursor
                        self.retirement["updated_at"] = time.time()
                        self._persist_retirement()
                    if self.step_delay:
                        time.sleep(self.step_delay)  # observable background progress
                    time.sleep(0)  # yield to concurrent API requests
                with self._ret_lock:
                    done = list(self.retirement.get("clones_materialized", []))
                    if name not in done:
                        done.append(name)
                    self.retirement.update({
                        "clones_materialized": done,
                        "materialize_cursor": 0,
                    })
                    self._persist_retirement()
                cursor = 0

            with self._ret_lock:
                if len(self.retirement.get("clones_materialized", [])) == len(CLONE_NAMES):
                    self.retirement.update({
                        "state": PHASE_INDEX_REMOVED,
                        "phase": PHASE_INDEX_REMOVED,
                        "resume_phase": PHASE_INDEX_REMOVED,
                        "updated_at": time.time(),
                    })
                    self._persist_retirement()
                    return

    # ---- phase 2: delete the template index (clones are now self-contained)
    def _retire_remove_index(self) -> None:
        with self._ret_lock:
            if self.retirement.get("state") not in (PHASE_INDEX_REMOVED, PHASE_GC_SWEEP,
                                                    PHASE_COMPLETED):
                return
        if self.template_index_path.exists():
            # Safety check: every clone must cover every former template block.
            if self.template_index is not None:
                for name in CLONE_NAMES:
                    clone = self.clones[name]
                    missing = [no for no in self.template_index if no not in clone.blocks]
                    if missing:
                        raise StorageError(
                            f"refusing to remove index: {name} missing materialized "
                            f"blocks {missing[:5]}{'...' if len(missing) > 5 else ''}")
            self._failpoint("before_index_remove")
            os.remove(self.template_index_path)
            _fsync_dir(self.template_dir)
            if self.step_delay:
                time.sleep(self.step_delay)
        self.template_index = None
        with self._ret_lock:
            self.retirement.update({
                "state": PHASE_GC_SWEEP,
                "phase": PHASE_GC_SWEEP,
                "index_removed": True,
                "resume_phase": PHASE_GC_SWEEP,
                "updated_at": time.time(),
            })
            self._persist_retirement()

    # ---- phase 3: garbage-collect store files with zero references
    def _retire_gc(self) -> None:
        with self._ret_lock:
            if self.retirement.get("state") not in (PHASE_GC_SWEEP, PHASE_COMPLETED):
                return
        # Global lock for the whole sweep: no new clone reference can appear
        # while we compute the live set and delete the rest. Small volume, so
        # this is a short critical section. Writes also clean up their own
        # orphaned files, so the store has no unreferenced blocks at rest.
        with self.global_lock:
            live = self._referenced_hashes()
            deleted = list(self.retirement.get("gc_deleted", []))
            for path in sorted(self.store_dir.iterdir()):
                if not path.is_file():
                    continue
                if path.name not in live:
                    self._failpoint("before_gc_delete")
                    os.remove(path)
                    _fsync_dir(self.store_dir)
                    if path.name not in deleted:
                        deleted.append(path.name)
                    if self.step_delay:
                        time.sleep(self.step_delay)
            with self._ret_lock:
                self.retirement["gc_deleted"] = sorted(deleted)
                self.retirement["updated_at"] = time.time()
                self._persist_retirement()

            # Final verification inside the same critical section: writers
            # cannot create new orphans concurrently with this check.
            on_disk = {p.name for p in self.store_dir.iterdir() if p.is_file()}
            if on_disk - self._referenced_hashes():
                raise StorageError("GC verification failed: unreferenced blocks remain")

    # ------------------------------------------------------------ test failpoints
    def _failpoint(self, name: str) -> None:
        """Raise a simulated retirement failure when FAILPOINT=<name>.

        Used by the test suite (and manual drills) to prove that failed
        retirements leave both clones fully usable and can be retried.
        """
        if os.environ.get("FAILPOINT") == name:
            raise StorageError(f"injected failure at failpoint {name!r}")
