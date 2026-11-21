"""FastAPI app: local block-storage drill with COW clones and template retirement."""

from __future__ import annotations

import base64
import binascii
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .storage import BLOCK_SIZE, CLONE_NAMES, BlockService, StorageError

DATA_DIR = os.environ.get("DATA_DIR", "/data")
BLOCK_COUNT = int(os.environ.get("BLOCK_COUNT", "32"))
TEMPLATE_SEED = os.environ.get("TEMPLATE_SEED", "template-disk-v1")
WEB_DIR = Path(__file__).resolve().parent.parent / "web"

service: BlockService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global service
    service = BlockService(DATA_DIR, BLOCK_COUNT, TEMPLATE_SEED)
    service.initialize()
    # A process restart resumes any in-flight retirement transparently.
    service.resume_retirement_on_startup()
    yield
    if service is not None:
        service.stop_worker()


app = FastAPI(title="Local Block Storage Drill", version="1.0.0", lifespan=lifespan)


@app.exception_handler(StorageError)
async def storage_error_handler(request: Request, exc: StorageError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": str(exc)})


def svc() -> BlockService:
    assert service is not None, "service not initialized"
    return service


class BlockWrite(BaseModel):
    data_hex: str | None = Field(default=None, description="exactly 4096 bytes, hex-encoded")
    data_base64: str | None = Field(default=None, description="exactly 4096 bytes, base64")
    pattern: str | None = Field(
        default=None,
        description="optional fill pattern: 'zero', 'ones' or a short ASCII string",
    )


def _decode_write(body: BlockWrite) -> bytes:
    if body.pattern is not None:
        if body.pattern == "zero":
            return b"\x00" * BLOCK_SIZE
        if body.pattern == "ones":
            return b"\xff" * BLOCK_SIZE
        raw = body.pattern.encode("utf-8")
        if not raw:
            raise StorageError("pattern must not be empty")
        reps = BLOCK_SIZE // len(raw) + 1
        return (raw * reps)[:BLOCK_SIZE]
    if body.data_hex is not None:
        try:
            data = bytes.fromhex(body.data_hex)
        except ValueError as exc:
            raise StorageError(f"invalid hex: {exc}") from exc
    elif body.data_base64 is not None:
        try:
            data = base64.b64decode(body.data_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise StorageError(f"invalid base64: {exc}") from exc
    else:
        raise StorageError("provide one of: data_hex, data_base64 or pattern")
    if len(data) != BLOCK_SIZE:
        raise StorageError(f"block data must be exactly {BLOCK_SIZE} bytes, got {len(data)}")
    return data


def _read_payload(read) -> dict:
    return {
        "block_no": int(read.ref.split(":")[1]) if read.ref and ":" in read.ref else None,
        "source": read.source,
        "ref": read.ref,
        "sha256": read.hash,
        "length": len(read.data),
        "data_hex": read.data.hex(),
        "data_base64": base64.b64encode(read.data).decode("ascii"),
        "preview_ascii": "".join(chr(b) if 32 <= b < 127 else "." for b in read.data[:64]),
    }


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/api/status")
async def status() -> dict:
    return svc().snapshot()


@app.get("/api/clones")
async def list_clones() -> dict:
    return {"clones": CLONE_NAMES, "block_count": svc().block_count, "block_size": BLOCK_SIZE}


@app.get("/api/clones/{name}/blocks/{block_no}")
async def read_clone_block(name: str, block_no: int) -> dict:
    return _read_payload(svc().read_block(name, block_no))


@app.put("/api/clones/{name}/blocks/{block_no}")
async def overwrite_clone_block(name: str, block_no: int, body: BlockWrite) -> dict:
    data = _decode_write(body)
    result = svc().overwrite_block(name, block_no, data)
    return {"ok": True, "operation": "overwrite", **result}


@app.delete("/api/clones/{name}/blocks/{block_no}")
async def discard_clone_block(name: str, block_no: int) -> dict:
    result = svc().discard_block(name, block_no)
    return {"ok": True, "operation": "discard", **result}


@app.get("/api/template/blocks/{block_no}")
async def read_template_block(block_no: int) -> Response:
    read = svc().read_template_block(block_no)
    if read is None:
        return JSONResponse(
            status_code=404,
            content={"error": "template index removed (retired) or block_no out of range"},
        )
    return JSONResponse(_read_payload(read))


@app.get("/api/retirement")
async def retirement_status() -> dict:
    return svc().retirement_status()


@app.post("/api/retirement/start")
async def start_retirement() -> dict:
    return svc().start_retirement()


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}
