import asyncio
import html
import logging
import mimetypes
import os
import re
import shutil
from typing import Optional
from urllib.parse import quote

from aiohttp import web
from telethon import TelegramClient
from telethon.tl.types import PeerChannel

logger = logging.getLogger("stream-server")

STREAM_PORT = int(os.getenv("STREAM_PORT", os.getenv("PORT", "8088")))
ALIGN_CHUNK = 512 * 1024  # 512 KB chunk alignment for Telethon iter_download

_user_client: Optional[TelegramClient] = None
_public_url: Optional[str] = None
_cf_process: Optional[asyncio.subprocess.Process] = None
_web_runner: Optional[web.AppRunner] = None

routes = web.RouteTableDef()


def get_public_url() -> str:
    return _public_url or f"http://127.0.0.1:{STREAM_PORT}"


def build_stream_links(chat_identifier: str, msg_id: int, file_name: str) -> dict:
    """Return direct stream URL for a given channel message."""
    base = get_public_url().rstrip("/")
    safe_name = quote(file_name or f"file_{msg_id}.mp4")
    return {
        "stream_url": f"{base}/stream/{chat_identifier}/{msg_id}/{safe_name}",
    }


async def _resolve_channel(chat_identifier: str):
    assert _user_client is not None
    clean = chat_identifier.strip()
    if clean.startswith("-100") and clean[4:].isdigit():
        clean = clean[4:]
    if clean.isdigit():
        peer = PeerChannel(int(clean))
        try:
            return await _user_client.get_entity(peer)
        except ValueError:
            await _user_client.get_dialogs()
            return await _user_client.get_entity(peer)
    return await _user_client.get_entity(clean)


async def _handle_media_stream(request: web.Request, disposition: str) -> web.StreamResponse:
    if _user_client is None:
        return web.Response(status=503, text="User client not initialized")

    chat_id_str = request.match_info["chat_id"]
    try:
        msg_id = int(request.match_info["msg_id"])
    except ValueError:
        return web.Response(status=400, text="Invalid message ID")

    try:
        entity = await _resolve_channel(chat_id_str)
        msg = await _user_client.get_messages(entity, ids=msg_id)
        if not msg or not msg.media or not getattr(msg, "file", None):
            return web.Response(status=404, text="Media not found in message")
    except Exception as e:
        logger.warning("Stream lookup failed for %s/%s: %s", chat_id_str, msg_id, e)
        return web.Response(status=404, text=f"Media not accessible: {e}")

    file_size = msg.file.size
    if not file_size:
        return web.Response(status=404, text="Unable to determine file size")

    file_name = (
        request.match_info.get("filename")
        or msg.file.name
        or f"file_{msg_id}{msg.file.ext or '.mp4'}"
    )
    mime_type = msg.file.mime_type or mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    # For inline streaming, ensure browsers play video containers smoothly
    if disposition == "inline" and (
        "matroska" in mime_type or file_name.lower().endswith((".mkv", ".mp4", ".webm", ".mov"))
    ):
        mime_type = "video/mp4"

    range_header = request.headers.get("Range")
    start = 0
    end = file_size - 1

    if range_header:
        try:
            unit, ranges = range_header.split("=", 1)
            if unit.strip().lower() == "bytes":
                first_range = ranges.split(",")[0].strip()
                start_str, end_str = first_range.split("-", 1)
                if start_str:
                    start = int(start_str)
                    if end_str:
                        end = int(end_str)
                elif end_str:
                    suffix_len = int(end_str)
                    start = max(0, file_size - suffix_len)
        except Exception:
            pass

    if start >= file_size or start < 0:
        return web.Response(
            status=416,
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    if end >= file_size:
        end = file_size - 1

    content_length = end - start + 1
    safe_disp_name = file_name.replace('"', "'")

    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(content_length),
        "Content-Disposition": f'{disposition}; filename="{safe_disp_name}"',
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Cache-Control": "no-cache",
    }

    status_code = 206 if range_header else 200
    response = web.StreamResponse(status=status_code, headers=headers)
    await response.prepare(request)

    if request.method == "HEAD":
        return response

    # Align Telethon download offset to 512 KB boundary
    aligned_offset = (start // ALIGN_CHUNK) * ALIGN_CHUNK
    skip_bytes = start - aligned_offset
    remaining = content_length

    try:
        async for chunk in _user_client.iter_download(
            msg.media,
            offset=aligned_offset,
            request_size=ALIGN_CHUNK,
        ):
            if not chunk:
                break

            if skip_bytes > 0:
                if len(chunk) <= skip_bytes:
                    skip_bytes -= len(chunk)
                    continue
                chunk = chunk[skip_bytes:]
                skip_bytes = 0

            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            if not chunk:
                break

            await response.write(chunk)
            remaining -= len(chunk)
            if remaining <= 0:
                break
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        pass
    except Exception as e:
        logger.debug("Stream interrupted for %s/%s: %s", chat_id_str, msg_id, e)

    return response


@routes.get("/stream/{chat_id}/{msg_id}")
@routes.get("/stream/{chat_id}/{msg_id}/{filename}")
async def stream_inline_handler(request: web.Request) -> web.StreamResponse:
    return await _handle_media_stream(request, disposition="inline")


async def _consume_stdout(stream: asyncio.StreamReader) -> None:
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
    except Exception:
        pass


async def start_cloudflare_tunnel(port: int) -> Optional[str]:
    global _cf_process
    cf_path = (
        shutil.which("cloudflared")
        or "/usr/local/bin/cloudflared"
        or "/usr/bin/cloudflared"
    )
    if not os.path.exists(cf_path):
        logger.warning("cloudflared binary not found; skipping Cloudflare tunnel.")
        return None

    cmd = [
        cf_path,
        "tunnel",
        "--metrics",
        "127.0.0.1:0",
        "--url",
        f"http://127.0.0.1:{port}",
    ]
    try:
        _cf_process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        logger.info("Starting Cloudflare Quick Tunnel for port %d...", port)
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        assert _cf_process.stdout is not None
        while loop.time() - start_time < 30:
            try:
                line = await asyncio.wait_for(_cf_process.stdout.readline(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            if not line:
                break
            line_str = line.decode("utf-8", errors="ignore").strip()
            match = re.search(r"(https://[a-zA-Z0-9.-]+\.trycloudflare\.com)", line_str)
            if match:
                url = match.group(1)
                asyncio.create_task(_consume_stdout(_cf_process.stdout))
                return url
    except Exception as e:
        logger.error("Failed starting cloudflared tunnel: %s", e)
    return None


async def restart_cloudflare_tunnel() -> Optional[str]:
    global _cf_process, _public_url
    if _cf_process:
        try:
            _cf_process.terminate()
            await _cf_process.wait()
        except Exception:
            pass
        _cf_process = None

    new_url = await start_cloudflare_tunnel(STREAM_PORT)
    if new_url:
        _public_url = new_url
    return new_url


async def start_stream_server(user_client: TelegramClient) -> str:
    """Start the aiohttp HTTP range-streaming server and Cloudflare Quick Tunnel."""
    global _user_client, _public_url, _web_runner
    _user_client = user_client

    app = web.Application()
    app.add_routes(routes)
    _web_runner = web.AppRunner(app)
    await _web_runner.setup()
    site = web.TCPSite(_web_runner, "0.0.0.0", STREAM_PORT)
    await site.start()
    logger.info("Stream HTTP server listening on http://0.0.0.0:%d", STREAM_PORT)

    env_public = os.getenv("PUBLIC_URL", "").strip()
    env_domain = os.getenv("DOMAIN", "").strip()
    if env_public:
        _public_url = env_public.rstrip("/")
        logger.info("Using configured PUBLIC_URL: %s", _public_url)
    elif env_domain:
        _public_url = (
            env_domain.rstrip("/")
            if env_domain.startswith(("http://", "https://"))
            else f"https://{env_domain.rstrip('/')}"
        )
        logger.info("Using configured DOMAIN: %s", _public_url)
    else:
        tunnel_url = await start_cloudflare_tunnel(STREAM_PORT)
        if tunnel_url:
            _public_url = tunnel_url
            logger.info("Cloudflare Public Stream URL: %s", _public_url)
        else:
            _public_url = f"http://138.2.111.21:{STREAM_PORT}"
            logger.warning("Falling back to IP stream URL: %s", _public_url)

    return _public_url


async def stop_stream_server() -> None:
    global _cf_process, _web_runner
    if _cf_process:
        try:
            _cf_process.terminate()
            await _cf_process.wait()
        except Exception:
            pass
        _cf_process = None
    if _web_runner:
        try:
            await _web_runner.cleanup()
        except Exception:
            pass
        _web_runner = None
