"""
Async client for the local WhatsApp sidecar (wa_sidecar/index.js, built on Baileys).

bot.py uses this module to:
  * auto-start the Node.js sidecar as a child process,
  * check the link status / fetch the login QR code,
  * list WhatsApp groups and resolve a group by JID or name,
  * upload downloaded files into a WhatsApp group.
"""
import asyncio
import logging
import mimetypes
import os
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("wa-client")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIDECAR_DIR = os.path.join(BASE_DIR, "wa_sidecar")

WA_SIDECAR_HOST = os.getenv("WA_SIDECAR_HOST", "127.0.0.1").strip() or "127.0.0.1"
WA_SIDECAR_PORT = int(os.getenv("WA_SIDECAR_PORT", "3100"))
WA_SIDECAR_URL = f"http://{WA_SIDECAR_HOST}:{WA_SIDECAR_PORT}"
WA_API_TOKEN = os.getenv("WA_API_TOKEN", "").strip()
WA_SIDECAR_AUTOSTART = os.getenv("WA_SIDECAR_AUTOSTART", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# WhatsApp size limits (MB). Media above these limits is sent as a document instead.
WA_VIDEO_MAX_MB = float(os.getenv("WA_VIDEO_MAX_MB", "100"))
WA_IMAGE_MAX_MB = float(os.getenv("WA_IMAGE_MAX_MB", "16"))
WA_AUDIO_MAX_MB = float(os.getenv("WA_AUDIO_MAX_MB", "16"))
# Absolute WhatsApp document limit; anything bigger is skipped.
WA_MAX_FILE_MB = float(os.getenv("WA_MAX_FILE_MB", "2000"))

_sidecar_process: Optional[asyncio.subprocess.Process] = None


class WhatsAppError(Exception):
    """Raised when the sidecar reports an error or is unreachable."""


@dataclass(frozen=True)
class WAGroup:
    id: str
    subject: str
    size: int


def _headers() -> Dict[str, str]:
    return {"x-api-key": WA_API_TOKEN} if WA_API_TOKEN else {}


async def _request(
    method: str,
    path: str,
    json_body: Optional[dict] = None,
    timeout: Optional[float] = 30,
    expect_bytes: bool = False,
) -> Any:
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    try:
        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.request(
                method, f"{WA_SIDECAR_URL}{path}", json=json_body, headers=_headers()
            ) as resp:
                if expect_bytes and resp.status == 200:
                    return await resp.read()
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = {"error": (await resp.text())[:500]}
                if resp.status >= 400:
                    raise WhatsAppError(data.get("error") or f"HTTP {resp.status}")
                return data
    except aiohttp.ClientConnectorError as e:
        raise WhatsAppError(
            f"WhatsApp sidecar is not reachable at {WA_SIDECAR_URL} ({e.strerror or e})"
        ) from e
    except asyncio.TimeoutError as e:
        raise WhatsAppError("Timed out talking to the WhatsApp sidecar") from e


# ---------------------------------------------------------------------------
# Sidecar process management
# ---------------------------------------------------------------------------


async def _pipe_sidecar_logs(stream: asyncio.StreamReader) -> None:
    while True:
        line = await stream.readline()
        if not line:
            break
        text = line.decode(errors="replace").rstrip()
        if text:
            logger.info("%s", text)


async def start_sidecar() -> bool:
    """Start the Node.js sidecar unless it's already running. Returns True if reachable."""
    global _sidecar_process

    try:
        await get_status()
        logger.info("WhatsApp sidecar already running at %s", WA_SIDECAR_URL)
        return True
    except WhatsAppError:
        pass

    if not WA_SIDECAR_AUTOSTART:
        logger.info("WhatsApp sidecar autostart disabled (WA_SIDECAR_AUTOSTART=false).")
        return False

    node_bin = shutil.which("node")
    entry = os.path.join(SIDECAR_DIR, "index.js")
    if not node_bin:
        logger.warning("Node.js not found in PATH; WhatsApp upload is disabled.")
        return False
    if not os.path.isdir(os.path.join(SIDECAR_DIR, "node_modules")):
        logger.warning(
            "WhatsApp sidecar dependencies missing. Run: cd %s && npm install", SIDECAR_DIR
        )
        return False

    env = dict(os.environ)
    env.setdefault("WA_SIDECAR_HOST", WA_SIDECAR_HOST)
    env.setdefault("WA_SIDECAR_PORT", str(WA_SIDECAR_PORT))

    _sidecar_process = await asyncio.create_subprocess_exec(
        node_bin,
        entry,
        cwd=SIDECAR_DIR,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert _sidecar_process.stdout is not None
    asyncio.create_task(_pipe_sidecar_logs(_sidecar_process.stdout))
    logger.info("Started WhatsApp sidecar (pid %s)", _sidecar_process.pid)

    for _ in range(30):
        await asyncio.sleep(0.5)
        if _sidecar_process.returncode is not None:
            logger.error("WhatsApp sidecar exited with code %s", _sidecar_process.returncode)
            return False
        try:
            await get_status()
            return True
        except WhatsAppError:
            continue
    logger.warning("WhatsApp sidecar did not respond within 15s.")
    return False


async def stop_sidecar() -> None:
    global _sidecar_process
    if _sidecar_process and _sidecar_process.returncode is None:
        try:
            _sidecar_process.terminate()
            await asyncio.wait_for(_sidecar_process.wait(), timeout=5)
        except Exception:
            try:
                _sidecar_process.kill()
            except Exception:
                pass
    _sidecar_process = None


# ---------------------------------------------------------------------------
# API wrappers
# ---------------------------------------------------------------------------


async def get_status() -> dict:
    return await _request("GET", "/status", timeout=5)


async def get_qr_png() -> Optional[bytes]:
    try:
        return await _request("GET", "/qr", timeout=10, expect_bytes=True)
    except WhatsAppError:
        return None


async def request_pairing_code(phone: str) -> str:
    data = await _request("POST", "/pair", {"phone": phone}, timeout=30)
    return data["code"]


async def logout() -> None:
    await _request("POST", "/logout", timeout=30)


async def list_groups(refresh: bool = False) -> List[WAGroup]:
    data = await _request("GET", f"/groups{'?refresh=1' if refresh else ''}", timeout=60)
    return [
        WAGroup(id=g["id"], subject=g.get("subject") or "", size=int(g.get("size") or 0))
        for g in data.get("groups", [])
    ]


async def resolve_group(selector: str, refresh: bool = False) -> WAGroup:
    """
    Resolve a group by exact JID (…@g.us), exact name, or unique partial name
    (case-insensitive). Raises WhatsAppError with a helpful message otherwise.
    """
    sel = selector.strip().strip("\"'")
    if not sel:
        raise WhatsAppError("No WhatsApp group specified.")

    groups = await list_groups(refresh=refresh)
    if sel.endswith("@g.us"):
        for g in groups:
            if g.id == sel:
                return g
        if not refresh:
            return await resolve_group(selector, refresh=True)
        raise WhatsAppError(f"The linked WhatsApp account is not a member of `{sel}`.")

    low = sel.lower()
    exact = [g for g in groups if g.subject.lower() == low]
    if len(exact) == 1:
        return exact[0]
    partial = exact or [g for g in groups if low in g.subject.lower()]
    if len(partial) == 1:
        return partial[0]
    if not partial:
        if not refresh:
            return await resolve_group(selector, refresh=True)
        raise WhatsAppError(f"No WhatsApp group matches `{sel}`. Use /wagroups to list groups.")
    names = ", ".join(f"`{g.subject}`" for g in partial[:8])
    raise WhatsAppError(f"`{sel}` matches multiple groups: {names}. Use the group ID instead.")


def choose_send_type(file_path: str, mime_type: Optional[str]) -> Optional[str]:
    """
    Pick how to send a file to WhatsApp:
      video/image/audio when supported & under the media limit, otherwise document.
    Returns None when the file exceeds WA_MAX_FILE_MB and must be skipped.
    """
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if size_mb > WA_MAX_FILE_MB:
        return None

    mime = (mime_type or mimetypes.guess_type(file_path)[0] or "").lower()
    if (mime == "video/mp4" or file_path.lower().endswith(".mp4")) and size_mb <= WA_VIDEO_MAX_MB:
        return "video"
    if mime in ("image/jpeg", "image/png") and size_mb <= WA_IMAGE_MAX_MB:
        return "image"
    if mime in ("audio/mpeg", "audio/mp4", "audio/ogg", "audio/aac") and size_mb <= WA_AUDIO_MAX_MB:
        return "audio"
    return "document"


async def send_file(
    jid: str,
    file_path: str,
    send_type: str,
    caption: str = "",
    file_name: Optional[str] = None,
    mime_type: Optional[str] = None,
    thumb_path: Optional[str] = None,
    seconds: Optional[float] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> dict:
    payload: Dict[str, Any] = {
        "jid": jid,
        "type": send_type,
        "path": os.path.abspath(file_path),
        "caption": caption or "",
        "fileName": file_name or os.path.basename(file_path),
    }
    mime = mime_type or mimetypes.guess_type(file_path)[0]
    if mime:
        payload["mimetype"] = mime
    if thumb_path and os.path.exists(thumb_path):
        payload["thumbPath"] = os.path.abspath(thumb_path)
    if seconds:
        payload["seconds"] = seconds
    if width and height:
        payload["width"] = width
        payload["height"] = height
    # Uploads of up to 2 GB can take a long time — no client-side timeout.
    return await _request("POST", "/send", payload, timeout=None)


async def send_text(jid: str, text: str) -> dict:
    return await _request("POST", "/text", {"jid": jid, "text": text}, timeout=60)
