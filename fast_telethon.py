import asyncio
import hashlib
import logging
import math
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from telethon import TelegramClient, errors, helpers
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InitConnectionRequest, InvokeWithLayerRequest
from telethon.tl.functions.auth import (
    ExportAuthorizationRequest,
    ImportAuthorizationRequest,
)
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.upload import (
    GetFileRequest,
    SaveBigFilePartRequest,
    SaveFilePartRequest,
)
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    TypeInputFile,
)

logger = logging.getLogger("fast-telethon")

CHUNK_SIZE = 512 * 1024  # 512 KB (maximum allowed by Telegram MTProto)
CONNECTIONS_PER_AUTH_KEY = 4  # Telegram allows 4 concurrent streams per foreign-DC AuthKey

MAX_DOWNLOAD_CONNECTIONS = int(os.getenv("MAX_DOWNLOAD_CONNECTIONS", "16"))
MAX_UPLOAD_CONNECTIONS = int(os.getenv("MAX_UPLOAD_CONNECTIONS", "8"))

# Cache authorized foreign-DC AuthKeys per (id(client), dc_id) so subsequent downloads
# connect in ~150ms without repeating DH key exchange or ExportAuthorizationRequest.
_EXPORTED_AUTH_KEYS: Dict[Tuple[int, int], List[AuthKey]] = {}
_AUTH_KEY_LOCK = asyncio.Lock()


def _get_connection_count(file_size: int, max_connections: int) -> int:
    """Choose optimal parallel MTProto connection count based on file size."""
    if file_size >= 50 * 1024 * 1024:   # >= 50 MB -> full 16 connections (4 AuthKeys x 4)
        return max_connections
    if file_size >= 15 * 1024 * 1024:   # >= 15 MB -> up to 12 connections (3 AuthKeys x 4)
        return min(12, max_connections)
    if file_size >= 3 * 1024 * 1024:    # >= 3 MB  -> up to 4 connections (1 AuthKey x 4)
        return min(4, max_connections)
    return 2


def _make_init_request(client: TelegramClient, query) -> InvokeWithLayerRequest:
    base = client._init_request
    init_req = InitConnectionRequest(
        api_id=client.api_id,
        device_model=base.device_model,
        system_version=base.system_version,
        app_version=base.app_version,
        lang_code=base.lang_code,
        system_lang_code=base.system_lang_code,
        lang_pack="",
        query=query,
        proxy=base.proxy,
    )
    return InvokeWithLayerRequest(LAYER, init_req)


async def _create_sender(
    client: TelegramClient, dc_id: int, auth_key: Optional[AuthKey] = None
) -> Tuple[MTProtoSender, AuthKey]:
    """Create and initialize a dedicated MTProtoSender for parallel file transfer."""
    dc = await client._get_dc(dc_id)
    is_same_dc = dc_id == client.session.dc_id

    if is_same_dc and auth_key is None:
        auth_key = client.session.auth_key

    sender = MTProtoSender(auth_key, loggers=client._log)
    await sender.connect(
        client._connection(
            dc.ip_address,
            dc.port,
            dc.id,
            loggers=client._log,
            proxy=client._proxy,
            local_addr=client._local_addr,
        )
    )

    try:
        if is_same_dc or auth_key is not None:
            req = _make_init_request(client, GetConfigRequest())
            await sender.send(req)
        else:
            auth = await client(ExportAuthorizationRequest(dc_id))
            req = _make_init_request(
                client, ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            )
            await sender.send(req)
    except errors.AuthKeyUnregisteredError:
        await sender.disconnect()
        if not is_same_dc and auth_key is not None:
            # Cached foreign AuthKey expired; generate a fresh one
            return await _create_sender(client, dc_id, auth_key=None)
        raise

    return sender, sender.auth_key


async def _get_or_create_key_group(
    client: TelegramClient,
    dc_id: int,
    group_size: int,
    cached_key: Optional[AuthKey] = None,
) -> Tuple[List[MTProtoSender], AuthKey]:
    """
    Create a group of up to `group_size` (4) parallel MTProtoSenders sharing one AuthKey.
    """
    first_sender, auth_key = await _create_sender(client, dc_id, auth_key=cached_key)
    if group_size <= 1:
        return [first_sender], auth_key

    rest_tasks = [
        _create_sender(client, dc_id, auth_key=auth_key)
        for _ in range(group_size - 1)
    ]
    results = await asyncio.gather(*rest_tasks, return_exceptions=True)

    senders = [first_sender]
    for res in results:
        if isinstance(res, Exception):
            logger.warning("Failed creating extra parallel sender in group: %s", res)
        else:
            sender, _ = res
            senders.append(sender)
    return senders, auth_key


async def _create_sender_pool(
    client: TelegramClient, dc_id: int, count: int
) -> List[MTProtoSender]:
    """
    Create a pool of `count` parallel MTProtoSenders.
    - On the home DC: all senders share the main session's AuthKey.
    - On a foreign DC: splits `count` into groups of 4 connections per exported AuthKey
      (e.g. 12 connections = 3 independent AuthKeys x 4 connections each) to bypass
      Telegram's 4-stream-per-AuthKey rate limit.
    """
    is_same_dc = dc_id == client.session.dc_id
    if is_same_dc:
        senders, _ = await _get_or_create_key_group(
            client, dc_id, count, cached_key=client.session.auth_key
        )
        return senders

    # Foreign DC: split into groups of CONNECTIONS_PER_AUTH_KEY (4)
    num_groups = math.ceil(count / CONNECTIONS_PER_AUTH_KEY)
    cache_key = (id(client), dc_id)

    async with _AUTH_KEY_LOCK:
        cached_keys = list(_EXPORTED_AUTH_KEYS.get(cache_key, []))

    group_sizes: List[int] = []
    remaining = count
    for _ in range(num_groups):
        sz = min(CONNECTIONS_PER_AUTH_KEY, remaining)
        group_sizes.append(sz)
        remaining -= sz

    all_senders: List[MTProtoSender] = []
    valid_keys: List[AuthKey] = []

    # Create key groups (sequentially when exporting new auth to avoid ExportAuthorization flood,
    # or in parallel when reusing cached keys)
    for idx, sz in enumerate(group_sizes):
        existing_key = cached_keys[idx] if idx < len(cached_keys) else None
        try:
            group_senders, used_key = await _get_or_create_key_group(
                client, dc_id, sz, cached_key=existing_key
            )
            all_senders.extend(group_senders)
            valid_keys.append(used_key)
        except Exception as e:
            logger.warning("Failed creating foreign DC sender group %d: %s", idx + 1, e)

    if valid_keys:
        async with _AUTH_KEY_LOCK:
            # Keep any additional previously cached keys beyond num_groups
            merged = list(valid_keys)
            for k in cached_keys[len(valid_keys) :]:
                merged.append(k)
            _EXPORTED_AUTH_KEYS[cache_key] = merged

    if not all_senders:
        raise RuntimeError(f"Could not establish any parallel connections to DC {dc_id}")

    logger.info(
        "Parallel pool ready on DC %d: %d connections across %d AuthKey(s)",
        dc_id,
        len(all_senders),
        len(valid_keys),
    )
    return all_senders


async def _close_sender_pool(senders: List[MTProtoSender]) -> None:
    await asyncio.gather(
        *(sender.disconnect() for sender in senders), return_exceptions=True
    )


async def fast_download_document(
    client: TelegramClient,
    document: Document,
    out_file_path: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    max_connections: int = MAX_DOWNLOAD_CONNECTIONS,
    msg_data: Optional[Tuple[Any, int]] = None,
) -> str:
    """
    Download a Telethon Document in parallel using up to 12 MTProto connections
    (grouped into 4 connections per AuthKey on foreign DCs) and 512 KB chunks.
    """
    file_size = document.size
    dc_id = document.dc_id
    location = InputDocumentFileLocation(
        id=document.id,
        access_hash=document.access_hash,
        file_reference=document.file_reference,
        thumb_size="",
    )

    part_size = CHUNK_SIZE
    total_parts = math.ceil(file_size / part_size)
    conn_count = min(
        _get_connection_count(file_size, max_connections),
        max(1, total_parts),
    )

    senders = await _create_sender_pool(client, dc_id, conn_count)
    fd = os.open(out_file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)

    next_part = 0
    downloaded_bytes = 0
    fileref_lock = asyncio.Lock()

    async def refresh_file_reference(old_ref: bytes) -> None:
        if not msg_data:
            return
        async with fileref_lock:
            if location.file_reference != old_ref:
                return
            chat, msg_id = msg_data
            logger.info(
                "File reference expired mid-download; refreshing from message #%s...",
                msg_id,
            )
            fresh_msg = await client.get_messages(chat, ids=msg_id)
            if fresh_msg and getattr(fresh_msg, "document", None):
                location.file_reference = fresh_msg.document.file_reference

    async def fetch_chunk(
        sender: MTProtoSender, offset: int, max_retries: int = 10
    ) -> bytes:
        attempt = 0
        while attempt < max_retries:
            cur_ref = location.file_reference
            req = GetFileRequest(location=location, offset=offset, limit=part_size)
            try:
                res = await sender.send(req)
                return res.bytes
            except (errors.FloodWaitError, errors.FloodPremiumWaitError) as e:
                if e.seconds > 60:
                    raise
                logger.debug(
                    "%s %ds at offset %d, sleeping...",
                    type(e).__name__,
                    e.seconds,
                    offset,
                )
                await asyncio.sleep(e.seconds + 0.25)
                # Do not increment `attempt` on normal non-premium bandwidth pacing
            except (
                errors.FilerefUpgradeNeededError,
                errors.FileReferenceExpiredError,
            ):
                attempt += 1
                if not msg_data or attempt >= max_retries:
                    raise
                await refresh_file_reference(cur_ref)
            except (
                errors.TimedOutError,
                errors.ServerError,
                asyncio.TimeoutError,
                ConnectionError,
                OSError,
            ) as e:
                attempt += 1
                if attempt >= max_retries:
                    raise
                wait_s = min(1.0 * attempt, 5.0)
                logger.info(
                    "Transient timeout/error on chunk offset %d (%s), retry %d/%d in %.1fs...",
                    offset,
                    type(e).__name__,
                    attempt,
                    max_retries,
                    wait_s,
                )
                await asyncio.sleep(wait_s)
        raise RuntimeError(f"Exhausted retries downloading chunk at offset {offset}")

    async def worker(sender: MTProtoSender) -> None:
        nonlocal next_part, downloaded_bytes
        while True:
            part_idx = next_part
            if part_idx >= total_parts:
                return
            next_part += 1

            offset = part_idx * part_size
            chunk = await fetch_chunk(sender, offset)
            os.pwrite(fd, chunk, offset)

            downloaded_bytes += len(chunk)
            if progress_callback:
                progress_callback(downloaded_bytes, file_size)

    try:
        await asyncio.gather(*(worker(s) for s in senders))
    finally:
        os.close(fd)
        await _close_sender_pool(senders)

    return out_file_path


async def fast_upload_file(
    client: TelegramClient,
    file_path: str,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    max_connections: int = MAX_UPLOAD_CONNECTIONS,
) -> TypeInputFile:
    """
    Upload a local file to Telegram in parallel using up to 8 MTProto connections
    and 512 KB chunks. Returns an InputFile / InputFileBig ready for client.send_file().
    """
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    file_id = helpers.generate_random_long()
    is_big = file_size > 10 * 1024 * 1024

    part_size = CHUNK_SIZE
    total_parts = math.ceil(file_size / part_size)
    conn_count = min(
        _get_connection_count(file_size, max_connections),
        max(1, total_parts),
    )

    dc_id = client.session.dc_id
    senders = await _create_sender_pool(client, dc_id, conn_count)
    fd = os.open(file_path, os.O_RDONLY)

    next_part = 0
    uploaded_bytes = 0

    async def upload_chunk(
        sender: MTProtoSender, part_idx: int, chunk: bytes, max_retries: int = 10
    ) -> None:
        attempt = 0
        while attempt < max_retries:
            if is_big:
                req = SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk)
            else:
                req = SaveFilePartRequest(file_id, part_idx, chunk)
            try:
                await sender.send(req)
                return
            except (errors.FloodWaitError, errors.FloodPremiumWaitError) as e:
                if e.seconds > 60:
                    raise
                await asyncio.sleep(e.seconds + 0.25)
            except (
                errors.TimedOutError,
                errors.ServerError,
                asyncio.TimeoutError,
                ConnectionError,
                OSError,
            ) as e:
                attempt += 1
                if attempt >= max_retries:
                    raise
                wait_s = min(1.0 * attempt, 5.0)
                logger.info(
                    "Transient timeout/error uploading part %d (%s), retry %d/%d in %.1fs...",
                    part_idx,
                    type(e).__name__,
                    attempt,
                    max_retries,
                    wait_s,
                )
                await asyncio.sleep(wait_s)

    async def worker(sender: MTProtoSender) -> None:
        nonlocal next_part, uploaded_bytes
        while True:
            part_idx = next_part
            if part_idx >= total_parts:
                return
            next_part += 1

            offset = part_idx * part_size
            chunk = os.pread(fd, part_size, offset)
            await upload_chunk(sender, part_idx, chunk)

            uploaded_bytes += len(chunk)
            if progress_callback:
                progress_callback(uploaded_bytes, file_size)

    try:
        await asyncio.gather(*(worker(s) for s in senders))
    finally:
        os.close(fd)
        await _close_sender_pool(senders)

    if is_big:
        return InputFileBig(file_id, total_parts, file_name)
    else:
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for block in iter(lambda: f.read(65536), b""):
                md5.update(block)
        return InputFile(file_id, total_parts, file_name, md5.hexdigest())
