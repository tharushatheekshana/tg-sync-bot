import asyncio
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Any, List, Optional, Set, Tuple

from dotenv import load_dotenv
from telethon import Button, TelegramClient, events, helpers, utils
from fast_telethon import fast_download_document, fast_upload_file
from stream_server import (
    build_stream_links,
    get_public_url,
    restart_cloudflare_tunnel,
    start_stream_server,
    stop_stream_server,
)
import wa_client
from wa_client import WAGroup, WhatsAppError
from sync_manager import (
    SyncManager,
    SyncRule,
    extract_media_unique_id,
    normalize_channel_id,
)
from payments import SubscriptionStore, TelegramStarsPayments
from telethon.errors import (
    ChannelPrivateError,
    ChannelsTooMuchError,
    FilePartsInvalidError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    InviteRequestSentError,
    MessageNotModifiedError,
    UserAlreadyParticipantError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.bots import SetBotCommandsRequest, SetBotInfoRequest
from telethon.tl.functions.channels import (
    EditAdminRequest,
    InviteToChannelRequest,
    JoinChannelRequest,
    LeaveChannelRequest,
)
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    ExportChatInviteRequest,
    ImportChatInviteRequest,
)
from telethon.tl.functions.upload import SaveBigFilePartRequest
from telethon.tl.types import (
    BotCommand,
    BotCommandScopeDefault,
    ChatAdminRights,
    ChatInviteAlready,
    ChatInvitePeek,
    MessageMediaWebPage,
    PeerChannel,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("channel-joiner")

# Configuration from .env
API_ID_RAW = os.getenv("API_ID", "").strip()
API_HASH = os.getenv("API_HASH", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
PAID_USER_IDS_RAW = os.getenv("PAID_USER_IDS", "").strip()
OWNER_IDS_RAW = os.getenv("OWNER_IDS", "").strip()
PUBLIC_MODE = os.getenv("PUBLIC_MODE", "true").strip().lower() in ("1", "true", "yes", "on")
FREE_MAX_TG_SYNC_RULES = max(1, int(os.getenv("FREE_MAX_TG_SYNC_RULES", "1") or "1"))
OWNER_CONTACT = os.getenv("OWNER_CONTACT", "").strip()
FORWARD_CHANNEL_ID_RAW = os.getenv("FORWARD_CHANNEL_ID", "4459969674").strip()
JOIN_DELAY_SECONDS = float(os.getenv("JOIN_DELAY_SECONDS", "3"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE_PATH = os.path.join(BASE_DIR, ".env")
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

if not API_ID_RAW or not API_HASH or not BOT_TOKEN:
    logger.error(
        "Missing required environment variables! Please set API_ID, API_HASH, and BOT_TOKEN in .env"
    )
    sys.exit(1)

try:
    API_ID = int(API_ID_RAW)
except ValueError:
    logger.error("API_ID must be an integer.")
    sys.exit(1)


def parse_admin_ids(raw: str) -> Set[int]:
    ids: Set[int] = set()
    for part in re.split(r"[,\s]+", raw):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids


def parse_admin_ids_ordered(raw: str) -> List[int]:
    ids: List[int] = []
    for part in re.split(r"[,\s]+", raw):
        part = part.strip()
        if part.lstrip("-").isdigit():
            val = int(part)
            if val not in ids:
                ids.append(val)
    return ids


# AUTHORIZED_USERS stores Paid/VIP users + Bot Owners
AUTHORIZED_USERS: Set[int] = parse_admin_ids(ADMIN_IDS_RAW) | parse_admin_ids(PAID_USER_IDS_RAW)
OWNER_IDS: Set[int] = parse_admin_ids(OWNER_IDS_RAW)
if not OWNER_IDS:
    ordered_admins = parse_admin_ids_ordered(ADMIN_IDS_RAW)
    if ordered_admins:
        OWNER_IDS.add(ordered_admins[0])
AUTHORIZED_USERS.update(OWNER_IDS)


def save_authorized_users_to_env() -> None:
    """Persist AUTHORIZED_USERS to ADMIN_IDS in .env so changes survive restarts."""
    ordered = sorted(OWNER_IDS) + sorted(AUTHORIZED_USERS - OWNER_IDS)
    admin_str = ", ".join(str(uid) for uid in ordered)
    if not os.path.exists(ENV_FILE_PATH):
        with open(ENV_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(f"ADMIN_IDS={admin_str}\n")
        return

    with open(ENV_FILE_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated = False
    new_lines: List[str] = []
    for line in lines:
        if re.match(r"^\s*ADMIN_IDS\s*=", line):
            new_lines.append(f"ADMIN_IDS={admin_str}\n")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines[-1] += "\n"
        new_lines.append(f"ADMIN_IDS={admin_str}\n")

    with open(ENV_FILE_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)


def parse_channel_id(raw: str) -> Tuple[int, int]:
    """Parse a channel ID string into (numeric_id, full_telethon_peer_id)."""
    s = str(raw).strip()
    if not s:
        return 0, 0
    if s.startswith("-100"):
        num = int(s[4:])
    elif s.startswith("-"):
        num = int(s[1:])
    elif s.isdigit():
        num = int(s)
    else:
        return 0, 0
    return num, int(f"-100{num}")


FORWARD_CHANNEL_NUM_ID, FORWARD_CHANNEL_PEER_ID = parse_channel_id(FORWARD_CHANNEL_ID_RAW)

# Default WhatsApp group JID (e.g. 1203630xxxxxxxxx@g.us) used by /wa when no group is given
WA_GROUP_ID = os.getenv("WA_GROUP_ID", "").strip()
# Delay between consecutive WhatsApp uploads (helps avoid WhatsApp spam detection)
WA_SEND_DELAY_SECONDS = float(os.getenv("WA_SEND_DELAY_SECONDS", "3"))
# Whether to send text-only posts or captions to WhatsApp (False by default to avoid spam and policy violations)
WA_SEND_TEXT = os.getenv("WA_SEND_TEXT", "false").strip().lower() in ("1", "true", "yes")
WA_SEND_CAPTIONS = os.getenv("WA_SEND_CAPTIONS", "false").strip().lower() in ("1", "true", "yes")
WA_SYNC_SEND_TEXT = os.getenv("WA_SYNC_SEND_TEXT", "true").strip().lower() in ("1", "true", "yes", "on")
WA_SYNC_SEND_CAPTIONS = os.getenv("WA_SYNC_SEND_CAPTIONS", "true").strip().lower() in ("1", "true", "yes", "on")

# Default Telegram destination channel/group (e.g. -1001234567890 or @channel) used by /tg and /tgsync
TG_UPLOAD_CHANNEL_ID = os.getenv("TG_UPLOAD_CHANNEL_ID", "").strip()
# Delay between consecutive Telegram channel uploads
TG_SEND_DELAY_SECONDS = float(os.getenv("TG_SEND_DELAY_SECONDS", "2"))
TG_SEND_TEXT = os.getenv("TG_SEND_TEXT", "true").strip().lower() in ("1", "true", "yes", "on")
TG_SEND_CAPTIONS = os.getenv("TG_SEND_CAPTIONS", "true").strip().lower() in ("1", "true", "yes", "on")
TG_SYNC_SEND_TEXT = os.getenv("TG_SYNC_SEND_TEXT", "true").strip().lower() in ("1", "true", "yes", "on")
TG_SYNC_SEND_CAPTIONS = os.getenv("TG_SYNC_SEND_CAPTIONS", "true").strip().lower() in ("1", "true", "yes", "on")

SYNC_CONFIG_PATH = os.path.join(BASE_DIR, "sync_config.json")
sync_manager = SyncManager(SYNC_CONFIG_PATH)
sync_queue: Optional[asyncio.Queue] = None


def save_env_value(key: str, value: str) -> None:
    """Create or update a single KEY=value line in .env so changes survive restarts."""
    lines: List[str] = []
    if os.path.exists(ENV_FILE_PATH):
        with open(ENV_FILE_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()

    pattern = re.compile(rf"^\s*{re.escape(key)}\s*=")
    updated = False
    new_lines: List[str] = []
    for line in lines:
        if pattern.match(line):
            new_lines.append(f"{key}={value}\n")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines[-1] += "\n"
        new_lines.append(f"{key}={value}\n")

    with open(ENV_FILE_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)


def save_forward_channel_to_env(channel_id_str: str) -> None:
    """Persist FORWARD_CHANNEL_ID in .env so changes survive restarts."""
    if not os.path.exists(ENV_FILE_PATH):
        with open(ENV_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(f"FORWARD_CHANNEL_ID={channel_id_str}\n")
        return

    with open(ENV_FILE_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated = False
    new_lines: List[str] = []
    for line in lines:
        if re.match(r"^\s*FORWARD_CHANNEL_ID\s*=", line):
            new_lines.append(f"FORWARD_CHANNEL_ID={channel_id_str}\n")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        if new_lines and not new_lines[-1].endswith("\n"):
            new_lines[-1] += "\n"
        new_lines.append(f"FORWARD_CHANNEL_ID={channel_id_str}\n")

    with open(ENV_FILE_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)



@dataclass(frozen=True)
class ParsedJoinTarget:
    raw: str
    kind: str  # "invite" | "public" | "channel_id"
    identifier: str  # invite_hash, username, or numeric channel_id string


@dataclass(frozen=True)
class ParsedPostLink:
    raw: str
    kind: str  # "private" | "public"
    chat_identifier: str  # numeric channel_id string (for /c/) or username
    msg_ids: Tuple[int, ...]  # one or more message IDs


def format_post_link(post: ParsedPostLink, msg_id: int) -> str:
    """Generate the direct t.me link for a specific message ID in a post."""
    if post.kind == "private":
        return f"https://t.me/c/{post.chat_identifier}/{msg_id}"
    return f"https://t.me/{post.chat_identifier}/{msg_id}"


# 1. Message/File Post Links:
# Matches:
# - https://t.me/c/4351613423/2
# - https://t.me/c/4351613423/10/25 (forum topic post)
# - https://t.me/c/4351613423/2-5 (range of messages)
PRIVATE_POST_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/(\d+)/(?:\d+/)?(\d+)(?:-(\d+))?",
    re.IGNORECASE,
)

# Matches public post links:
# - https://t.me/channel_name/123
# - https://t.me/channel_name/10/123
# - https://t.me/channel_name/123-128
PUBLIC_POST_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?:s/)?([a-zA-Z][a-zA-Z0-9_]{3,31})/(?:\d+/)?(\d+)(?:-(\d+))?",
    re.IGNORECASE,
)

# 2. Channel Join Links:
PRIVATE_INVITE_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?:\+|joinchat/)([a-zA-Z0-9_-]+)"
    r"|tg://join\?invite=([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)

PUBLIC_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?:s/)?([a-zA-Z][a-zA-Z0-9_]{3,31})(?:/\d+)?(?:\?[^\s]*)?",
    re.IGNORECASE,
)

PRIVATE_CHANNEL_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/(\d+)(?:/[\d-]+)*/?",
    re.IGNORECASE,
)

USERNAME_RE = re.compile(r"(?<!\w)@([a-zA-Z][a-zA-Z0-9_]{3,31})\b")

CHANNEL_ID_RE = re.compile(
    r"(?<![a-zA-Z0-9_+-])(?:-100(\d{5,15})|-(\d{5,15})|(\d{6,15}))\b"
)

RESERVED_PATHS = {
    "joinchat",
    "addstickers",
    "addemoji",
    "addlist",
    "share",
    "proxy",
    "socks",
    "bg",
    "setlanguage",
    "c",
    "s",
    "iv",
    "login",
    "confirmphone",
}

MAX_RANGE_MESSAGES = int(os.getenv("MAX_BULK_MESSAGES", "500"))
BULK_DELAY_SECONDS = float(os.getenv("BULK_DELAY_SECONDS", "3"))
BULK_COOLDOWN_EVERY = int(os.getenv("BULK_COOLDOWN_EVERY", "10"))
BULK_COOLDOWN_SECONDS = float(os.getenv("BULK_COOLDOWN_SECONDS", "15"))

# Track chats that requested cancellation of an active bulk download
CANCELLED_CHATS: Set[int] = set()
ACTIVE_BULK_CHATS: Set[int] = set()


async def _cancellable_sleep(chat_id: int, seconds: float) -> None:
    """Sleep in up to 1s increments so /cancel is responsive during cooldowns."""
    remaining = seconds
    while remaining > 0 and chat_id not in CANCELLED_CHATS:
        step = min(1.0, remaining)
        await asyncio.sleep(step)
        remaining -= step


async def _apply_bulk_cooldown(
    chat_id: int,
    status_msg,
    sent_count: int,
    batch_prefix: str,
    last_sent_link: Optional[str] = None,
) -> None:
    """Apply per-item cooldown and periodic batch cooldown during bulk operations."""
    if chat_id in CANCELLED_CHATS:
        return
    last_link_str = f"\n🔗 **Last Sent:** {last_sent_link}" if last_sent_link else ""
    if (
        BULK_COOLDOWN_EVERY > 0
        and BULK_COOLDOWN_SECONDS > 0
        and sent_count > 0
        and sent_count % BULK_COOLDOWN_EVERY == 0
    ):
        try:
            await status_msg.edit(
                f"⏳ {batch_prefix}**Cooling down for {int(BULK_COOLDOWN_SECONDS)}s** "
                f"(after `{sent_count}` items) to avoid Telegram rate limits..."
                f"{last_link_str}",
                link_preview=False,
            )
        except Exception:
            pass
        await _cancellable_sleep(chat_id, BULK_COOLDOWN_SECONDS)
    elif BULK_DELAY_SECONDS > 0:
        await _cancellable_sleep(chat_id, BULK_DELAY_SECONDS)

# Matches two links separated by '-', 'to', or whitespace when using /bulk or /batch:
# e.g. https://t.me/c/4351613423/2 - https://t.me/c/4351613423/50
TWO_PRIVATE_LINKS_RANGE_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/(\d+)/(?:\d+/)?(\d+)"
    r"\s*(?:-|–|—|\bto\b)\s*"
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/c/\1/(?:\d+/)?(\d+)",
    re.IGNORECASE,
)

TWO_PUBLIC_LINKS_RANGE_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?:s/)?([a-zA-Z][a-zA-Z0-9_]{3,31})/(?:\d+/)?(\d+)"
    r"\s*(?:-|–|—|\bto\b)\s*"
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?:s/)?\1/(?:\d+/)?(\d+)",
    re.IGNORECASE,
)


def _expand_msg_range(start_str: str, end_str: Optional[str]) -> Tuple[int, ...]:
    start_id = int(start_str)
    if not end_str:
        return (start_id,)
    end_id = int(end_str)
    if end_id < start_id:
        start_id, end_id = end_id, start_id
    if end_id - start_id + 1 > MAX_RANGE_MESSAGES:
        end_id = start_id + MAX_RANGE_MESSAGES - 1
    return tuple(range(start_id, end_id + 1))


def extract_post_links(text: str) -> List[ParsedPostLink]:
    """
    Extract private (t.me/c/<id>/<msg>) and public (t.me/<user>/<msg>) post links,
    supporting single links, range links (t.me/c/id/2-50), two-link ranges
    (link1 - link2 or /bulk link1 link2), and multiple pasted links.
    """
    posts: List[ParsedPostLink] = []
    seen: Set[Tuple[str, str, Tuple[int, ...]]] = set()
    working = text

    # 0. Check if `/bulk <link1> <link2>` or `/batch <link1> <link2>` with two links from the same channel
    is_bulk_cmd = working.lstrip().lower().startswith(
        ("/bulk", "/batch", "/wa", "/whatsapp", "/tg", "/tgupload", "/upload")
    )

    for match in TWO_PRIVATE_LINKS_RANGE_RE.finditer(working):
        channel_id = match.group(1)
        msg_ids = _expand_msg_range(match.group(2), match.group(3))
        key = ("private", channel_id, msg_ids)
        if key not in seen:
            seen.add(key)
            posts.append(
                ParsedPostLink(
                    raw=match.group(0),
                    kind="private",
                    chat_identifier=channel_id,
                    msg_ids=msg_ids,
                )
            )
    working = TWO_PRIVATE_LINKS_RANGE_RE.sub(" ", working)

    for match in TWO_PUBLIC_LINKS_RANGE_RE.finditer(working):
        username = match.group(1)
        if username.lower() in RESERVED_PATHS:
            continue
        msg_ids = _expand_msg_range(match.group(2), match.group(3))
        key = ("public", username.lower(), msg_ids)
        if key not in seen:
            seen.add(key)
            posts.append(
                ParsedPostLink(
                    raw=match.group(0),
                    kind="public",
                    chat_identifier=username,
                    msg_ids=msg_ids,
                )
            )
    working = TWO_PUBLIC_LINKS_RANGE_RE.sub(" ", working)

    # 1. Private channel message links: t.me/c/4351613423/2 or t.me/c/4351613423/2-50
    for match in PRIVATE_POST_RE.finditer(working):
        channel_id = match.group(1)
        msg_ids = _expand_msg_range(match.group(2), match.group(3))
        key = ("private", channel_id, msg_ids)
        if key not in seen:
            seen.add(key)
            posts.append(
                ParsedPostLink(
                    raw=match.group(0),
                    kind="private",
                    chat_identifier=channel_id,
                    msg_ids=msg_ids,
                )
            )

    cleaned = PRIVATE_POST_RE.sub(" ", working)

    # 2. Public channel message links: t.me/username/123 or t.me/username/123-150
    for match in PUBLIC_POST_RE.finditer(cleaned):
        username = match.group(1)
        if username.lower() in RESERVED_PATHS:
            continue
        msg_ids = _expand_msg_range(match.group(2), match.group(3))
        key = ("public", username.lower(), msg_ids)
        if key not in seen:
            seen.add(key)
            posts.append(
                ParsedPostLink(
                    raw=match.group(0),
                    kind="public",
                    chat_identifier=username,
                    msg_ids=msg_ids,
                )
            )

    # If user ran `/bulk <link1> <link2>` with exactly 2 single links from the same channel,
    # treat them as a start-to-end range!
    if (
        is_bulk_cmd
        and len(posts) == 2
        and len(posts[0].msg_ids) == 1
        and len(posts[1].msg_ids) == 1
        and posts[0].kind == posts[1].kind
        and posts[0].chat_identifier.lower() == posts[1].chat_identifier.lower()
    ):
        range_ids = _expand_msg_range(
            str(posts[0].msg_ids[0]), str(posts[1].msg_ids[0])
        )
        return [
            ParsedPostLink(
                raw=f"{posts[0].raw} -> {posts[1].raw}",
                kind=posts[0].kind,
                chat_identifier=posts[0].chat_identifier,
                msg_ids=range_ids,
            )
        ]

    return posts


def extract_join_targets(text: str) -> List[ParsedJoinTarget]:
    """Extract private invite links, public channel links, @usernames, and channel IDs to join."""
    targets: List[ParsedJoinTarget] = []
    seen: Set[Tuple[str, str]] = set()

    # 1. Match private invite links first
    for match in PRIVATE_INVITE_RE.finditer(text):
        invite_hash = match.group(1) or match.group(2)
        key = ("invite", invite_hash)
        if invite_hash and key not in seen:
            seen.add(key)
            targets.append(
                ParsedJoinTarget(raw=match.group(0), kind="invite", identifier=invite_hash)
            )

    cleaned_text = PRIVATE_INVITE_RE.sub(" ", text)

    # 2. Match private t.me/c/<channel_id> links
    for match in PRIVATE_CHANNEL_LINK_RE.finditer(cleaned_text):
        channel_id = match.group(1)
        key = ("channel_id", channel_id)
        if channel_id and key not in seen:
            seen.add(key)
            targets.append(
                ParsedJoinTarget(raw=match.group(0), kind="channel_id", identifier=channel_id)
            )

    cleaned_text = PRIVATE_CHANNEL_LINK_RE.sub(" ", cleaned_text)

    # 3. Match public t.me/<username> links
    for match in PUBLIC_LINK_RE.finditer(cleaned_text):
        username = match.group(1)
        if username.lower() in RESERVED_PATHS:
            continue
        key = ("public", username.lower())
        if key not in seen:
            seen.add(key)
            targets.append(
                ParsedJoinTarget(raw=match.group(0), kind="public", identifier=username)
            )

    cleaned_text = PUBLIC_LINK_RE.sub(" ", cleaned_text)

    # 4. Match @username mentions
    for match in USERNAME_RE.finditer(cleaned_text):
        username = match.group(1)
        key = ("public", username.lower())
        if key not in seen:
            seen.add(key)
            targets.append(
                ParsedJoinTarget(raw=match.group(0), kind="public", identifier=username)
            )

    cleaned_text = USERNAME_RE.sub(" ", cleaned_text)

    # 5. Match numeric channel IDs (e.g. -1001234567890, -1234567890, or 1234567890)
    for match in CHANNEL_ID_RE.finditer(cleaned_text):
        channel_id = match.group(1) or match.group(2) or match.group(3)
        key = ("channel_id", channel_id)
        if channel_id and key not in seen:
            seen.add(key)
            targets.append(
                ParsedJoinTarget(raw=match.group(0), kind="channel_id", identifier=channel_id)
            )

    return targets


def format_bytes(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"


def make_progress_callback(
    status_msg,
    action_label: str,
    loop: asyncio.AbstractEventLoop,
    footer: str = "",
):
    """Create a throttled progress callback for Telethon download/upload."""
    start_time = time.monotonic()
    last_edit_time = [0.0]

    async def _edit_progress(current: int, total: int) -> None:
        if not total:
            return
        now = time.monotonic()
        # Update at most once every 3.5 seconds to avoid Telegram FloodWait
        if now - last_edit_time[0] < 3.5 and current < total:
            return
        last_edit_time[0] = now

        elapsed = max(now - start_time, 0.1)
        speed = current / elapsed
        pct = (current / total) * 100
        filled = int(pct // 10)
        bar = "█" * filled + "░" * (10 - filled)

        text = (
            f"{action_label}\n"
            f"`[{bar}] {pct:.1f}%`\n"
            f"📦 `{format_bytes(current)} / {format_bytes(total)}`\n"
            f"⚡ `{format_bytes(speed)}/s`"
            f"{footer}"
        )
        try:
            await status_msg.edit(text, link_preview=False)
        except Exception:
            pass

    def _callback(current: int, total: int) -> None:
        loop.create_task(_edit_progress(current, total))

    return _callback


async def resolve_post_entity(user_client: TelegramClient, post: ParsedPostLink):
    """Resolve the channel/group entity for a private (/c/...) or public post link."""
    if post.kind == "private":
        channel_id = int(post.chat_identifier)
        peer = PeerChannel(channel_id)
        try:
            return await user_client.get_entity(peer)
        except ValueError:
            # Refresh dialogs cache in case the user account recently joined
            await user_client.get_dialogs()
            return await user_client.get_entity(peer)
    else:
        return await user_client.get_entity(post.chat_identifier)


async def download_message_media(
    user_client: TelegramClient,
    msg,
    entity,
    temp_dir: str,
    raw_name: str,
    progress_callback,
) -> Optional[str]:
    """
    Download a message's media into temp_dir. Uses parallel multi-connection download
    for documents >= 1 MB and falls back to Telethon's standard download.
    """
    doc = getattr(msg, "document", None)
    file_path = None
    if doc is not None and doc.size >= 1024 * 1024:
        out_path = os.path.join(temp_dir, os.path.basename(raw_name))
        try:
            file_path = await fast_download_document(
                user_client,
                doc,
                out_path,
                progress_callback=progress_callback,
                msg_data=(entity, msg.id),
            )
        except Exception as fast_err:
            logger.warning(
                "Parallel download failed (%s), falling back to standard download",
                fast_err,
            )
            file_path = None

    if not file_path:
        file_path = await user_client.download_media(
            msg,
            file=temp_dir,
            progress_callback=progress_callback,
        )
    return file_path


async def fetch_and_send_post(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    event: events.NewMessage.Event,
    post: ParsedPostLink,
    force_stream_only: bool = False,
    force_telegram_upload: bool = False,
) -> None:
    """
    Fetch media/text from a channel message link using user_client.
    - For big files (>= BIG_FILE_THRESHOLD_MB) or /stream: sends instant public stream & download links.
    - For smaller files (< BIG_FILE_THRESHOLD_MB) or /dl: downloads & sends the file directly in Telegram chat.
    """
    status_msg = await event.reply(f"🔍 Fetching message from `{post.raw}`...")
    loop = asyncio.get_running_loop()

    try:
        entity = await resolve_post_entity(user_client, post)
    except (ValueError, ChannelPrivateError):
        await status_msg.edit(
            f"❌ **Cannot access channel `{post.chat_identifier}`.**\n"
            "Make sure the bot has joined this channel first (send its invite link to join)."
        )
        return
    except Exception as e:
        await status_msg.edit(
            f"❌ **Error resolving channel `{post.chat_identifier}`:** `{type(e).__name__}: {e}`"
        )
        return

    total_count = len(post.msg_ids)
    is_bulk = total_count > 1
    sent_count = 0
    last_sent_link: Optional[str] = None
    owns_bulk_session = is_bulk and (event.chat_id not in ACTIVE_BULK_CHATS)

    if owns_bulk_session:
        ACTIVE_BULK_CHATS.add(event.chat_id)
        CANCELLED_CHATS.discard(event.chat_id)
    if is_bulk:
        await status_msg.edit(
            f"📦 **Bulk Download Started ({total_count} messages)**\n"
            "Send `/cancel` anytime to stop."
        )

    try:
        for idx, msg_id in enumerate(post.msg_ids, start=1):
            if event.chat_id in CANCELLED_CHATS:
                msg_text = f"🛑 **Bulk download cancelled** after `{sent_count}` file(s)."
                if last_sent_link:
                    msg_text += f"\n🔗 **Last Sent Link:** {last_sent_link}"
                await bot_client.send_message(
                    event.chat_id,
                    msg_text,
                    reply_to=event.id,
                    link_preview=False,
                )
                break

            batch_prefix = f"**[{idx}/{total_count}]** " if is_bulk else ""
            item_sent = False
            flood_retries = 0
            while flood_retries < 3 and event.chat_id not in CANCELLED_CHATS:
                temp_dir = tempfile.mkdtemp(prefix=f"msg_{msg_id}_", dir=DOWNLOAD_DIR)
                try:
                    msg = await user_client.get_messages(entity, ids=msg_id)
                    if not msg:
                        if not is_bulk:
                            await bot_client.send_message(
                                event.chat_id,
                                f"⚠️ Message `#{msg_id}` was not found or has been deleted in `{post.chat_identifier}`.",
                                reply_to=event.id,
                            )
                        break

                    has_file_media = bool(msg.media) and not isinstance(
                        msg.media, MessageMediaWebPage
                    )

                    if not has_file_media:
                        if msg.message:
                            await bot_client.send_message(
                                event.chat_id,
                                msg.message,
                                formatting_entities=msg.entities,
                                reply_to=event.id,
                            )
                            sent_count += 1
                            item_sent = True
                            last_sent_link = format_post_link(post, msg_id)
                        elif not is_bulk:
                            await bot_client.send_message(
                                event.chat_id,
                                f"ℹ️ Message `#{msg_id}` contains no downloadable file or text.",
                                reply_to=event.id,
                            )
                        break

                    file_obj = getattr(msg, "file", None)
                    file_size = getattr(file_obj, "size", 0) or 0
                    raw_name = (file_obj and file_obj.name) or (
                        f"file_{msg_id}{utils.get_extension(msg.media) or '.mp4'}"
                    )
                    links = build_stream_links(post.chat_identifier, msg_id, raw_name)

                    async def send_stream_message(reason_note: str = "") -> None:
                        caption_preview = (
                            f"\n📝 **Caption:** {msg.message[:300]}" if msg.message else ""
                        )
                        header_note = f"{reason_note}\n\n" if reason_note else ""
                        stream_reply = (
                            f"🎬 {batch_prefix}**Stream Ready!**\n"
                            f"{header_note}"
                            f"📄 **File:** `{raw_name}`\n"
                            f"📦 **Size:** `{format_bytes(file_size)}`"
                            f"{caption_preview}\n\n"
                            f"🔗 {links['stream_url']}"
                        )
                        await bot_client.send_message(
                            event.chat_id,
                            stream_reply,
                            reply_to=event.id,
                            link_preview=False,
                        )

                    if force_stream_only:
                        await send_stream_message()
                        sent_count += 1
                        item_sent = True
                        break

                    # Ask Telegram directly if bot_client is allowed to upload this number of 512 KB parts
                    total_parts = math.ceil(file_size / (512 * 1024)) if file_size else 1
                    if total_parts > 1000:
                        try:
                            await bot_client(
                                SaveBigFilePartRequest(
                                    helpers.generate_random_long(),
                                    total_parts - 1,
                                    total_parts,
                                    b"\x00",
                                )
                            )
                        except FilePartsInvalidError:
                            logger.info(
                                "Telegram rejected %s (%d parts) with FilePartsInvalidError; sending stream link.",
                                raw_name,
                                total_parts,
                            )
                            await send_stream_message(
                                "⚠️ _Telegram rejected this file as too large for bot upload — providing direct stream & download link instead:_"
                            )
                            sent_count += 1
                            item_sent = True
                            last_sent_link = format_post_link(post, msg_id)
                            break
                        except Exception:
                            pass

                    # Download media using user_client (use parallel multi-connection for documents >= 1 MB)
                    last_link_str = f"\n🔗 **Last Sent:** {last_sent_link}" if last_sent_link else ""
                    dl_label = f"⬇️ {batch_prefix}**Downloading `{raw_name}` (`#{msg_id}`)...**"
                    await status_msg.edit(dl_label + last_link_str, link_preview=False)
                    dl_progress = make_progress_callback(
                        status_msg,
                        dl_label,
                        loop,
                        footer=last_link_str,
                    )
                    doc = getattr(msg, "document", None)
                    file_path = await download_message_media(
                        user_client, msg, entity, temp_dir, raw_name, dl_progress
                    )

                    if not file_path or not os.path.exists(file_path):
                        await bot_client.send_message(
                            event.chat_id,
                            f"❌ Failed to download media from message `#{msg_id}`.",
                            reply_to=event.id,
                        )
                        break

                    if event.chat_id in CANCELLED_CHATS:
                        break

                    actual_size = os.path.getsize(file_path)
                    mime_type = getattr(file_obj, "mime_type", None)
                    is_video = (
                        bool(mime_type and mime_type.startswith("video/"))
                        or file_path.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".avi", ".ts", ".m4v"))
                    )

                    # Fast remux with +faststart for instant streaming & thumbnail rendering
                    if is_video and actual_size <= 2000 * 1024 * 1024:
                        base_stem = os.path.splitext(os.path.basename(file_path))[0]
                        fast_target = os.path.join(temp_dir, f"{base_stem}_fast.mp4")
                        try:
                            remux_proc = await asyncio.create_subprocess_exec(
                                "ffmpeg", "-y", "-i", file_path, "-c", "copy", "-movflags", "+faststart",
                                fast_target,
                                stdout=asyncio.subprocess.DEVNULL,
                                stderr=asyncio.subprocess.DEVNULL,
                            )
                            await asyncio.wait_for(remux_proc.communicate(), timeout=30.0)
                            if remux_proc.returncode == 0 and os.path.exists(fast_target) and os.path.getsize(fast_target) > 0:
                                try:
                                    os.remove(file_path)
                                except OSError:
                                    pass
                                file_path = fast_target
                                actual_size = os.path.getsize(file_path)
                        except Exception as remux_err:
                            logger.debug("Faststart remux skipped: %s", remux_err)

                    # Optionally download thumbnail to preserve video/file preview
                    thumb_path = None
                    try:
                        thumb_candidate = os.path.join(temp_dir, "thumb.jpg")
                        thumb_path = await user_client.download_media(
                            msg, thumb=-1, file=thumb_candidate
                        )
                    except Exception:
                        thumb_path = None

                    # If Telegram had no thumbnail, extract a clean frame at 1s via FFmpeg
                    if (not thumb_path or not os.path.exists(thumb_path)) and is_video:
                        extracted_thumb = os.path.join(temp_dir, "thumb.jpg")
                        try:
                            extract_proc = await asyncio.create_subprocess_exec(
                                "ffmpeg", "-y", "-ss", "00:00:01", "-i", file_path,
                                "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "4",
                                extracted_thumb,
                                stdout=asyncio.subprocess.DEVNULL,
                                stderr=asyncio.subprocess.DEVNULL,
                            )
                            await asyncio.wait_for(extract_proc.communicate(), timeout=8.0)
                            if (
                                extract_proc.returncode == 0
                                and os.path.exists(extracted_thumb)
                                and os.path.getsize(extracted_thumb) > 0
                            ):
                                thumb_path = extracted_thumb
                        except Exception as extract_err:
                            logger.debug("ffmpeg thumbnail extraction failed: %s", extract_err)

                    # Upload media to user via bot_client (use parallel multi-connection upload for files >= 1 MB)
                    file_basename = os.path.basename(file_path)
                    last_link_str = f"\n🔗 **Last Sent:** {last_sent_link}" if last_sent_link else ""
                    ul_label = f"⬆️ {batch_prefix}**Uploading `{file_basename}`...**"
                    await status_msg.edit(ul_label + last_link_str, link_preview=False)
                    ul_progress = make_progress_callback(
                        status_msg,
                        ul_label,
                        loop,
                        footer=last_link_str,
                    )

                    try:
                        upload_handle = file_path
                        if actual_size >= 1024 * 1024:
                            try:
                                upload_handle = await fast_upload_file(
                                    bot_client,
                                    file_path,
                                    progress_callback=ul_progress,
                                )
                            except FilePartsInvalidError:
                                raise
                            except Exception as fast_ul_err:
                                logger.warning(
                                    "Parallel upload failed (%s), falling back to standard upload",
                                    fast_ul_err,
                                )
                                upload_handle = file_path

                        attributes = doc.attributes if doc is not None else None
                        await bot_client.send_file(
                            event.chat_id,
                            file=upload_handle,
                            caption=msg.message or "",
                            formatting_entities=msg.entities,
                            attributes=attributes,
                            thumb=thumb_path,
                            supports_streaming=True,
                            reply_to=event.id,
                            progress_callback=ul_progress if upload_handle == file_path else None,
                        )
                        sent_count += 1
                        item_sent = True
                        last_sent_link = format_post_link(post, msg_id)
                    except FilePartsInvalidError:
                        logger.info(
                            "Telegram rejected upload for %s as too large (FilePartsInvalidError); sending stream link.",
                            raw_name,
                        )
                        await send_stream_message(
                            "⚠️ _Telegram rejected this file as too large for bot upload — providing direct stream & download link instead:_"
                        )
                        sent_count += 1
                        item_sent = True
                        last_sent_link = format_post_link(post, msg_id)
                    except Exception as ul_err:
                        if (
                            "FILE_PARTS_INVALID" in str(ul_err)
                            or "too big" in str(ul_err).lower()
                            or "too large" in str(ul_err).lower()
                        ):
                            await send_stream_message(
                                "⚠️ _Telegram rejected this file as too large for bot upload — providing direct stream & download link instead:_"
                            )
                            sent_count += 1
                            item_sent = True
                            last_sent_link = format_post_link(post, msg_id)
                        else:
                            raise
                    break

                except FloodWaitError as e:
                    flood_retries += 1
                    wait_sec = e.seconds + 2
                    last_link_str = f"\n🔗 **Last Sent:** {last_sent_link}" if last_sent_link else ""
                    try:
                        await status_msg.edit(
                            f"⚠️ {batch_prefix}**Telegram FloodWait:** Cooling down for **{wait_sec}s** before retrying `#{msg_id}`..."
                            f"{last_link_str}",
                            link_preview=False,
                        )
                    except Exception:
                        pass
                    await _cancellable_sleep(event.chat_id, wait_sec)
                except Exception as e:
                    logger.exception("Failed processing message %s from %s", msg_id, post.raw)
                    await bot_client.send_message(
                        event.chat_id,
                        f"❌ Error processing `{post.raw}` (`#{msg_id}`): `{type(e).__name__}: {e}`",
                        reply_to=event.id,
                    )
                    break
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)

            if event.chat_id in CANCELLED_CHATS:
                msg_text = f"🛑 **Bulk download cancelled** after `{sent_count}` file(s)."
                if last_sent_link:
                    msg_text += f"\n🔗 **Last Sent Link:** {last_sent_link}"
                await bot_client.send_message(
                    event.chat_id,
                    msg_text,
                    reply_to=event.id,
                    link_preview=False,
                )
                break

            if is_bulk and idx < total_count:
                if item_sent:
                    await _apply_bulk_cooldown(
                        event.chat_id, status_msg, sent_count, batch_prefix, last_sent_link=last_sent_link
                    )
                else:
                    await _cancellable_sleep(event.chat_id, 0.4)
    finally:
        if owns_bulk_session:
            ACTIVE_BULK_CHATS.discard(event.chat_id)
            CANCELLED_CHATS.discard(event.chat_id)

    try:
        await status_msg.delete()
    except Exception:
        pass


def strip_post_links(text: str) -> str:
    """Remove all Telegram post links (and range separators) from text, returning the rest."""
    working = text
    for pattern in (
        TWO_PRIVATE_LINKS_RANGE_RE,
        TWO_PUBLIC_LINKS_RANGE_RE,
        PRIVATE_POST_RE,
        PUBLIC_POST_RE,
    ):
        working = pattern.sub(" ", working)
    # Drop leftover separators such as "-", "–", "—" or "," between links
    working = re.sub(r"(?:^|\s)[-–—,]+(?=\s|$)", " ", working)
    return " ".join(working.split())


async def send_posts_to_whatsapp(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    event: events.NewMessage.Event,
    posts: List[ParsedPostLink],
    group: WAGroup,
) -> None:
    """
    Bulk-download every message referenced by `posts` with the user account and upload
    each file (or text) to the WhatsApp group via the local Baileys sidecar.
    Progress and a final summary are reported in the Telegram chat.
    """
    chat_id = event.chat_id
    loop = asyncio.get_running_loop()
    total = sum(len(p.msg_ids) for p in posts)
    group_label = group.subject or group.id

    status_msg = await event.reply(
        f"📲 **WhatsApp Upload Started** → **{group_label}**\n"
        f"📦 `{total}` message(s) queued. Send `/cancel` anytime to stop."
    )

    ACTIVE_BULK_CHATS.add(chat_id)
    CANCELLED_CHATS.discard(chat_id)

    sent = 0
    skipped: List[str] = []
    failed: List[str] = []
    aborted_reason = ""
    last_sent_link: Optional[str] = None
    last_sent_msg_id: Optional[int] = None
    idx = 0

    try:
        for post in posts:
            if chat_id in CANCELLED_CHATS or aborted_reason:
                break
            try:
                entity = await resolve_post_entity(user_client, post)
            except Exception as e:
                failed.append(
                    f"`{post.chat_identifier}`: cannot access channel ({type(e).__name__})"
                )
                idx += len(post.msg_ids)
                continue

            for msg_id in post.msg_ids:
                idx += 1
                if chat_id in CANCELLED_CHATS or aborted_reason:
                    break
                prefix = f"**[{idx}/{total}]**"
                item_sent = False
                flood_retries = 0

                while flood_retries < 3 and chat_id not in CANCELLED_CHATS:
                    temp_dir = tempfile.mkdtemp(prefix=f"wa_{msg_id}_", dir=DOWNLOAD_DIR)
                    try:
                        msg = await user_client.get_messages(entity, ids=msg_id)
                        if not msg:
                            skipped.append(f"`#{msg_id}`: not found / deleted")
                            break

                        has_file_media = bool(msg.media) and not isinstance(
                            msg.media, MessageMediaWebPage
                        )

                        # Text-only post -> skip completely unless WA_SEND_TEXT is explicitly enabled
                        if not has_file_media:
                            if WA_SEND_TEXT and msg.message:
                                await wa_client.send_text(group.id, msg.message)
                                sent += 1
                                item_sent = True
                                last_sent_link = format_post_link(post, msg_id)
                                last_sent_msg_id = msg_id
                            else:
                                skipped.append(f"`#{msg_id}`: text-only post (skipped)")
                            break

                        file_obj = getattr(msg, "file", None)
                        file_size = getattr(file_obj, "size", 0) or 0
                        raw_name = (file_obj and file_obj.name) or (
                            f"file_{msg_id}{utils.get_extension(msg.media) or '.bin'}"
                        )
                        unique_file_id = extract_media_unique_id(msg)
                        if unique_file_id and sync_manager.is_wa_file_uploaded(group.id, unique_file_id):
                            skipped.append(
                                f"`#{msg_id}` `{raw_name}`: duplicate file (`{unique_file_id}`) already uploaded to this WhatsApp group"
                            )
                            break

                        if file_size and file_size > wa_client.WA_MAX_FILE_MB * 1024 * 1024:
                            skipped.append(
                                f"`#{msg_id}` `{raw_name}`: {format_bytes(file_size)} exceeds "
                                f"WhatsApp's {int(wa_client.WA_MAX_FILE_MB)} MB limit"
                            )
                            break

                        last_link_str = f"\n🔗 **Last Sent:** {last_sent_link}" if last_sent_link else ""
                        dl_label = f"⬇️ {prefix} **Downloading `{raw_name}` (`#{msg_id}`)...**"
                        await status_msg.edit(dl_label + last_link_str, link_preview=False)
                        file_path = await download_message_media(
                            user_client,
                            msg,
                            entity,
                            temp_dir,
                            raw_name,
                            make_progress_callback(status_msg, dl_label, loop, footer=last_link_str),
                        )
                        if not file_path or not os.path.exists(file_path):
                            failed.append(f"`#{msg_id}`: download failed")
                            break
                        if chat_id in CANCELLED_CHATS:
                            break

                        actual_size = os.path.getsize(file_path)
                        mime_type = getattr(file_obj, "mime_type", None)
                        is_video = (
                            bool(mime_type and mime_type.startswith("video/"))
                            or file_path.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".avi", ".ts", ".m4v"))
                        )

                        # If it's a video under WhatsApp's video limit, remux with +faststart
                        # so the MP4 moov atom is at the front for instant streaming & preview cards.
                        if is_video and actual_size <= wa_client.WA_VIDEO_MAX_MB * 1024 * 1024:
                            base_stem = os.path.splitext(os.path.basename(file_path))[0]
                            fast_target = os.path.join(temp_dir, f"{base_stem}_fast.mp4")
                            try:
                                remux_proc = await asyncio.create_subprocess_exec(
                                    "ffmpeg", "-y", "-i", file_path, "-c", "copy", "-movflags", "+faststart",
                                    fast_target,
                                    stdout=asyncio.subprocess.DEVNULL,
                                    stderr=asyncio.subprocess.DEVNULL,
                                )
                                await asyncio.wait_for(remux_proc.communicate(), timeout=30.0)
                                if remux_proc.returncode == 0 and os.path.exists(fast_target) and os.path.getsize(fast_target) > 0:
                                    try:
                                        os.remove(file_path)
                                    except OSError:
                                        pass
                                    file_path = fast_target
                                    actual_size = os.path.getsize(file_path)
                                    mime_type = "video/mp4"
                            except Exception as remux_err:
                                logger.debug("Faststart remux skipped: %s", remux_err)

                        send_type = wa_client.choose_send_type(file_path, mime_type)
                        if send_type is None:
                            skipped.append(
                                f"`#{msg_id}` `{raw_name}`: {format_bytes(actual_size)} exceeds "
                                f"WhatsApp's {int(wa_client.WA_MAX_FILE_MB)} MB limit"
                            )
                            break

                        # Extract video duration and dimensions if missing
                        duration = getattr(file_obj, "duration", None)
                        width = getattr(file_obj, "width", None)
                        height = getattr(file_obj, "height", None)
                        if is_video and (not width or not height or not duration):
                            try:
                                probe_proc = await asyncio.create_subprocess_exec(
                                    "ffprobe", "-v", "error", "-select_streams", "v:0",
                                    "-show_entries", "stream=width,height,duration",
                                    "-of", "csv=p=0", file_path,
                                    stdout=asyncio.subprocess.PIPE,
                                    stderr=asyncio.subprocess.DEVNULL,
                                )
                                stdout, _ = await asyncio.wait_for(probe_proc.communicate(), timeout=5.0)
                                if probe_proc.returncode == 0 and stdout:
                                    parts = stdout.decode("utf-8", "ignore").strip().split(",")
                                    if len(parts) >= 2:
                                        if not width and parts[0].isdigit():
                                            width = int(parts[0])
                                        if not height and parts[1].isdigit():
                                            height = int(parts[1])
                                    if len(parts) >= 3 and not duration:
                                        try:
                                            duration = float(parts[2])
                                        except ValueError:
                                            pass
                            except Exception as probe_err:
                                logger.debug("ffprobe metadata extraction skipped: %s", probe_err)

                        thumb_path = None
                        try:
                            thumb_path = await user_client.download_media(
                                msg, thumb=-1, file=os.path.join(temp_dir, "thumb.jpg")
                            )
                        except Exception:
                            thumb_path = None

                        # Fallback: If Telegram had no thumbnail, extract a clean frame at 1s via FFmpeg
                        if (not thumb_path or not os.path.exists(thumb_path)) and is_video:
                            extracted_thumb = os.path.join(temp_dir, "thumb.jpg")
                            seek_time = "00:00:01" if (duration and duration >= 2) else "00:00:00"
                            try:
                                extract_proc = await asyncio.create_subprocess_exec(
                                    "ffmpeg", "-y", "-ss", seek_time, "-i", file_path,
                                    "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "4",
                                    extracted_thumb,
                                    stdout=asyncio.subprocess.DEVNULL,
                                    stderr=asyncio.subprocess.DEVNULL,
                                )
                                await asyncio.wait_for(extract_proc.communicate(), timeout=8.0)
                                if (
                                    extract_proc.returncode == 0
                                    and os.path.exists(extracted_thumb)
                                    and os.path.getsize(extracted_thumb) > 0
                                ):
                                    thumb_path = extracted_thumb
                            except Exception as extract_err:
                                logger.debug("ffmpeg thumbnail extraction failed: %s", extract_err)

                        file_name = os.path.basename(file_path)
                        last_link_str = f"\n🔗 **Last sent:** {last_sent_link}" if last_sent_link else ""
                        await status_msg.edit(
                            f"⬆️ {prefix} **Uploading `{file_name}` to WhatsApp** "
                            f"as {send_type} (`{format_bytes(actual_size)}`)...\n"
                            f"👥 Group: **{group_label}**"
                            f"{last_link_str}",
                            link_preview=False,
                        )
                        media_caption = (msg.message or "") if WA_SEND_CAPTIONS else ""
                        await wa_client.send_file(
                            group.id,
                            file_path,
                            send_type,
                            caption=media_caption,
                            file_name=file_name,
                            mime_type=mime_type,
                            thumb_path=thumb_path,
                            seconds=duration,
                            width=width,
                            height=height,
                        )
                        sync_manager.mark_wa_file_uploaded(group.id, unique_file_id)
                        sent += 1
                        item_sent = True
                        last_sent_link = format_post_link(post, msg_id)
                        last_sent_msg_id = msg_id
                        break

                    except FloodWaitError as e:
                        flood_retries += 1
                        wait_sec = e.seconds + 2
                        last_link_str = f"\n🔗 **Last sent:** {last_sent_link}" if last_sent_link else ""
                        try:
                            await status_msg.edit(
                                f"⚠️ {prefix} **Telegram FloodWait:** cooling down **{wait_sec}s** "
                                f"before retrying `#{msg_id}`..."
                                f"{last_link_str}",
                                link_preview=False,
                            )
                        except Exception:
                            pass
                        await _cancellable_sleep(chat_id, wait_sec)
                    except WhatsAppError as e:
                        failed.append(f"`#{msg_id}`: WhatsApp error: {e}")
                        if "not connected" in str(e).lower() or "not reachable" in str(e).lower():
                            aborted_reason = str(e)
                        break
                    except Exception as e:
                        logger.exception("WhatsApp upload failed for %s #%s", post.raw, msg_id)
                        failed.append(f"`#{msg_id}`: {type(e).__name__}: {e}")
                        break
                    finally:
                        shutil.rmtree(temp_dir, ignore_errors=True)

                if idx < total and chat_id not in CANCELLED_CHATS and not aborted_reason:
                    if item_sent:
                        if (
                            BULK_COOLDOWN_EVERY > 0
                            and BULK_COOLDOWN_SECONDS > 0
                            and sent % BULK_COOLDOWN_EVERY == 0
                        ):
                            last_link_str = f"\n🔗 **Last sent:** {last_sent_link}" if last_sent_link else ""
                            try:
                                await status_msg.edit(
                                    f"📲 {prefix} **Cooling down {int(BULK_COOLDOWN_SECONDS)}s** "
                                    f"after `{sent}` uploads to avoid rate limits..."
                                    f"{last_link_str}",
                                    link_preview=False,
                                )
                            except Exception:
                                pass
                            await _cancellable_sleep(chat_id, BULK_COOLDOWN_SECONDS)
                        elif WA_SEND_DELAY_SECONDS > 0:
                            await _cancellable_sleep(chat_id, WA_SEND_DELAY_SECONDS)
                    else:
                        await _cancellable_sleep(chat_id, 0.4)
    finally:
        was_cancelled = chat_id in CANCELLED_CHATS
        ACTIVE_BULK_CHATS.discard(chat_id)
        CANCELLED_CHATS.discard(chat_id)

    if aborted_reason:
        header = f"⚠️ **WhatsApp upload aborted:** {aborted_reason}"
    elif was_cancelled:
        header = "🛑 **WhatsApp upload cancelled.**"
    else:
        header = "✅ **WhatsApp upload finished.**"

    lines = [
        header,
        f"👥 Group: **{group_label}**",
        f"📤 Sent: `{sent}` / `{total}`",
    ]
    if last_sent_link:
        lines.append(f"🔗 **Last Sent Link:** {last_sent_link}")
        if was_cancelled and last_sent_msg_id:
            next_link = format_post_link(post, last_sent_msg_id + 1)
            lines.append(f"▶️ **Resume with:** `/wa {next_link}`")
    max_detail = 15
    if skipped:
        lines.append(f"\n⏭️ **Skipped ({len(skipped)}):**")
        lines.extend(f"• {s}" for s in skipped[:max_detail])
        if len(skipped) > max_detail:
            lines.append(f"• …and {len(skipped) - max_detail} more")
    if failed:
        lines.append(f"\n❌ **Failed ({len(failed)}):**")
        lines.extend(f"• {f}" for f in failed[:max_detail])
        if len(failed) > max_detail:
            lines.append(f"• …and {len(failed) - max_detail} more")

    try:
        await status_msg.delete()
    except Exception:
        pass
    await bot_client.send_message(chat_id, "\n".join(lines), reply_to=event.id, link_preview=False)


async def join_single_target(
    user_client: TelegramClient,
    target: ParsedJoinTarget,
    bot_client: Optional[TelegramClient] = None,
) -> Tuple[bool, str]:
    """
    Use the user_client (MTProto user account) to join a single target channel/group.
    Returns (success, status_message).
    """
    try:
        if target.kind == "invite":
            updates = await user_client(ImportChatInviteRequest(target.identifier))
            chat_title = (
                updates.chats[0].title
                if getattr(updates, "chats", None)
                else "Private Channel/Group"
            )
            chat_id = updates.chats[0].id if getattr(updates, "chats", None) else "N/A"
            return True, f"✅ Joined **{chat_title}** (`-100{chat_id}`) via invite link."

        elif target.kind == "channel_id":
            num_id, _ = parse_channel_id(target.identifier)
            if not num_id:
                return False, f"❌ Invalid channel ID: `{target.raw}`"

            peer = PeerChannel(num_id)
            entity = None
            try:
                entity = await user_client.get_entity(peer)
            except ValueError:
                try:
                    await user_client.get_dialogs()
                    entity = await user_client.get_entity(peer)
                except Exception:
                    entity = None
            except Exception:
                entity = None

            if entity is None and bot_client is not None:
                try:
                    bot_entity = await bot_client.get_entity(peer)
                    bot_uname = getattr(bot_entity, "username", None)
                    if bot_uname:
                        entity = await user_client.get_entity(bot_uname)
                    else:
                        inv = await bot_client(ExportChatInviteRequest(peer))
                        inv_link = getattr(inv, "link", None)
                        if inv_link:
                            inv_match = PRIVATE_INVITE_RE.search(inv_link)
                            if inv_match:
                                inv_hash = inv_match.group(1) or inv_match.group(2)
                                updates = await user_client(ImportChatInviteRequest(inv_hash))
                                chat_title = (
                                    updates.chats[0].title
                                    if getattr(updates, "chats", None)
                                    else getattr(bot_entity, "title", f"-100{num_id}")
                                )
                                chat_id = (
                                    updates.chats[0].id
                                    if getattr(updates, "chats", None)
                                    else num_id
                                )
                                return (
                                    True,
                                    f"✅ Joined **{chat_title}** (ID: `-100{chat_id}`).",
                                )
                except UserAlreadyParticipantError:
                    return True, f"ℹ️ Already a member of channel (`-100{num_id}`)."
                except Exception:
                    pass

            if entity is None:
                return (
                    False,
                    f"❌ Could not join channel ID `{target.raw}` directly (Telegram requires an invite link `https://t.me/+...` or `@username` for unknown private channels, or add the bot to the channel first).",
                )

            chat_title = getattr(entity, "title", None) or f"-100{num_id}"
            chat_id = getattr(entity, "id", num_id)
            chat_uname = getattr(entity, "username", None)
            uname_tag = f"`@{chat_uname}`, " if chat_uname else ""

            if getattr(entity, "left", True) is False:
                return (
                    True,
                    f"ℹ️ Already a member of **{chat_title}** ({uname_tag}ID: `-100{chat_id}`).",
                )

            updates = await user_client(JoinChannelRequest(entity))
            if getattr(updates, "chats", None):
                chat_title = getattr(updates.chats[0], "title", chat_title)
                chat_id = getattr(updates.chats[0], "id", chat_id)
            return (
                True,
                f"✅ Joined **{chat_title}** ({uname_tag}ID: `-100{chat_id}`).",
            )

        else:
            updates = await user_client(JoinChannelRequest(target.identifier))
            chat_title = (
                updates.chats[0].title
                if getattr(updates, "chats", None)
                else f"@{target.identifier}"
            )
            chat_id = updates.chats[0].id if getattr(updates, "chats", None) else "N/A"
            return (
                True,
                f"✅ Joined **{chat_title}** (`@{target.identifier}`, ID: `-100{chat_id}`).",
            )

    except UserAlreadyParticipantError:
        title_info = target.raw
        if target.kind == "invite":
            try:
                invite_info = await user_client(CheckChatInviteRequest(target.identifier))
                if isinstance(invite_info, (ChatInviteAlready, ChatInvitePeek)) and getattr(
                    invite_info, "chat", None
                ):
                    title_info = f"**{invite_info.chat.title}** (`-100{invite_info.chat.id}`)"
            except Exception:
                pass
        return True, f"ℹ️ Already a member of {title_info}."

    except InviteRequestSentError:
        return (
            True,
            f"⏳ Join request sent for `{target.raw}` (waiting for admin approval).",
        )

    except (InviteHashExpiredError, InviteHashInvalidError):
        return False, f"❌ Invite link expired or invalid: `{target.raw}`"

    except (UsernameInvalidError, UsernameNotOccupiedError):
        return False, f"❌ No channel/group found for username: `{target.raw}`"

    except ChannelPrivateError:
        return (
            False,
            f"❌ Channel `{target.raw}` is private or inaccessible (send its invite link `https://t.me/+...` to join).",
        )

    except ChannelsTooMuchError:
        return (
            False,
            "❌ Maximum number of joined channels/supergroups has been reached.",
        )

    except FloodWaitError as e:
        return (
            False,
            f"⚠️ Telegram FloodWait: Must wait **{e.seconds}s** before joining more channels.",
        )

    except ValueError as e:
        return False, f"❌ Could not resolve `{target.raw}`: {e}"

    except Exception as e:
        logger.exception("Unexpected error joining %s", target.raw)
        return False, f"❌ Error joining `{target.raw}`: `{type(e).__name__}: {e}`"


async def leave_single_target(user_client: TelegramClient, identifier: str) -> Tuple[bool, str]:
    """Leave a channel or group by link, @username, or numeric ID."""
    targets = extract_join_targets(identifier)
    if targets and targets[0].kind == "channel_id":
        num_id, _ = parse_channel_id(targets[0].identifier)
        target_entity: Any = PeerChannel(num_id) if num_id else identifier.strip()
    elif targets and targets[0].kind == "public":
        target_entity = targets[0].identifier
    else:
        target_entity = identifier.strip()

    try:
        if isinstance(target_entity, str) and target_entity.lstrip("-").isdigit():
            num_id, _ = parse_channel_id(target_entity)
            entity = await user_client.get_entity(PeerChannel(num_id) if num_id else int(target_entity))
        else:
            entity = await user_client.get_entity(target_entity)

        title = getattr(entity, "title", str(identifier))
        await user_client(LeaveChannelRequest(entity))
        return True, f"👋 Left **{title}**."
    except Exception as e:
        return False, f"❌ Failed to leave `{identifier}`: `{type(e).__name__}: {e}`"


# ----------------------------------------------------------------------
# Auto-Sync Helper Functions
# ----------------------------------------------------------------------


async def resolve_channel_for_sync(
    user_client: TelegramClient,
    target_str: str,
    bot_client: Optional[TelegramClient] = None,
) -> Tuple[Optional[Any], str]:
    """
    Resolve a channel entity from username (@channel or channel), public URL,
    invite link (joins if needed), message link (t.me/c/... or t.me/.../123),
    or numeric ID (-100... or ...).
    Returns (entity, error_message).
    """
    s = target_str.strip()
    if not s:
        return None, "No channel specified."

    # 1. Private invite link (e.g. https://t.me/+InviteHash, https://t.me/joinchat/...)
    inv_match = PRIVATE_INVITE_RE.search(s)
    if inv_match:
        invite_hash = inv_match.group(1) or inv_match.group(2)
        try:
            updates = await user_client(ImportChatInviteRequest(invite_hash))
            if getattr(updates, "chats", None):
                return updates.chats[0], ""
        except UserAlreadyParticipantError:
            try:
                invite_info = await user_client(CheckChatInviteRequest(invite_hash))
                if isinstance(invite_info, (ChatInviteAlready, ChatInvitePeek)) and getattr(
                    invite_info, "chat", None
                ):
                    return invite_info.chat, ""
            except Exception:
                pass
        except InviteRequestSentError:
            return None, "Join request sent for this channel (waiting for admin approval)."
        except Exception as e:
            logger.debug("Invite import error: %s", e)

    # 2. Channel message link or t.me/c/<id> link (e.g. https://t.me/c/1234567890/10 or https://t.me/c/1234567890)
    posts = extract_post_links(s)
    if posts:
        try:
            entity = await resolve_post_entity(user_client, posts[0])
            if entity:
                if getattr(entity, "left", False):
                    try:
                        await user_client(JoinChannelRequest(entity))
                    except Exception:
                        pass
                return entity, ""
        except Exception as e:
            logger.debug("Post entity resolution error: %s", e)

    priv_ch_match = PRIVATE_CHANNEL_LINK_RE.search(s)
    if priv_ch_match:
        s = priv_ch_match.group(1)

    # 3. Numeric Channel ID (e.g. -1001234567890 or 1234567890)
    num_id, _ = parse_channel_id(s)
    if num_id:
        peer = PeerChannel(num_id)
        entity = None
        try:
            entity = await user_client.get_entity(peer)
        except ValueError:
            try:
                await user_client.get_dialogs()
                entity = await user_client.get_entity(peer)
            except Exception:
                entity = None
        except Exception as e:
            logger.debug("Numeric ID resolution error: %s", e)

        if entity is None and bot_client is not None:
            try:
                bot_entity = await bot_client.get_entity(peer)
                bot_uname = getattr(bot_entity, "username", None)
                if bot_uname:
                    try:
                        updates = await user_client(JoinChannelRequest(bot_uname))
                        if getattr(updates, "chats", None):
                            return updates.chats[0], ""
                    except Exception:
                        entity = await user_client.get_entity(bot_uname)
                else:
                    inv = await bot_client(ExportChatInviteRequest(peer))
                    inv_link = getattr(inv, "link", None)
                    if inv_link:
                        inv_m = PRIVATE_INVITE_RE.search(inv_link)
                        if inv_m:
                            inv_h = inv_m.group(1) or inv_m.group(2)
                            updates = await user_client(ImportChatInviteRequest(inv_h))
                            if getattr(updates, "chats", None):
                                return updates.chats[0], ""
            except Exception:
                pass

        if entity is not None:
            if getattr(entity, "left", False):
                try:
                    updates = await user_client(JoinChannelRequest(entity))
                    if getattr(updates, "chats", None):
                        return updates.chats[0], ""
                except Exception:
                    pass
            return entity, ""

    # 4. Public link or @username
    pub_match = PUBLIC_LINK_RE.search(s)
    if pub_match:
        username = pub_match.group(1)
    else:
        username = s.lstrip("@").strip()

    if username and username.lower() not in RESERVED_PATHS:
        try:
            entity = await user_client.get_entity(username)
            return entity, ""
        except ValueError:
            try:
                # Try joining public channel
                updates = await user_client(JoinChannelRequest(username))
                if getattr(updates, "chats", None):
                    return updates.chats[0], ""
            except Exception:
                pass
        except Exception as e:
            logger.debug("Username resolution error: %s", e)

    # 5. Last resort: Search loaded dialogs by title or username
    try:
        dialogs = await user_client.get_dialogs()
        low_s = s.strip("\"'").lower()
        partial_match = None
        for d in dialogs:
            if not (getattr(d, "is_channel", False) or getattr(d, "is_group", False)):
                continue
            d_uname = (getattr(d.entity, "username", "") or "").lower()
            d_title = (getattr(d, "title", "") or "").lower()
            if username and d_uname and d_uname == username.lower():
                return d.entity, ""
            if low_s and low_s == d_title:
                return d.entity, ""
            if low_s and low_s in d_title and partial_match is None:
                partial_match = d.entity
        if partial_match is not None:
            return partial_match, ""
    except Exception as e:
        logger.debug("Dialog search error: %s", e)

    return (
        None,
        f"Could not access channel `{s}`. Ensure your Telegram account has joined this channel.",
    )


@dataclass(frozen=True)
class TGChannelInfo:
    entity: Any
    id: int
    peer_id: int
    title: str
    username: Optional[str]
    is_creator: bool
    is_admin: bool
    can_post: bool
    is_megagroup: bool


async def list_user_telegram_channels(
    user_client: TelegramClient, query: str = "", only_writable: bool = False
) -> List[TGChannelInfo]:
    """List Telegram channels and supergroups from the user's dialogs."""
    dialogs = await user_client.get_dialogs()
    items: List[TGChannelInfo] = []
    q_low = query.strip().lower()

    for d in dialogs:
        if not (getattr(d, "is_channel", False) or getattr(d, "is_group", False)):
            continue
        ent = d.entity
        title = getattr(d, "title", None) or getattr(ent, "title", None) or str(ent.id)
        uname = getattr(ent, "username", None)
        peer_id = utils.get_peer_id(ent)
        is_creator = bool(getattr(ent, "creator", False))
        admin_rights = getattr(ent, "admin_rights", None)
        is_admin = bool(admin_rights)
        is_broadcast = bool(getattr(ent, "broadcast", False))
        is_megagroup = bool(getattr(ent, "megagroup", False)) or not is_broadcast
        banned_rights = getattr(ent, "banned_rights", None)

        if is_broadcast:
            can_post = is_creator or bool(admin_rights and getattr(admin_rights, "post_messages", False))
        else:
            can_post = is_creator or is_admin or not bool(
                banned_rights and getattr(banned_rights, "send_messages", False)
            )

        if only_writable and not can_post:
            continue

        if q_low:
            if (
                q_low not in title.lower()
                and (not uname or q_low not in uname.lower())
                and q_low not in str(peer_id)
                and q_low not in str(ent.id)
            ):
                continue

        items.append(
            TGChannelInfo(
                entity=ent,
                id=ent.id,
                peer_id=peer_id,
                title=title,
                username=uname,
                is_creator=is_creator,
                is_admin=is_admin,
                can_post=can_post,
                is_megagroup=is_megagroup,
            )
        )

    items.sort(
        key=lambda c: (
            0 if c.is_creator else (1 if c.is_admin else (2 if c.can_post else 3)),
            c.title.lower(),
        )
    )
    return items


async def _send_text_to_tg_dest(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    dest_entity: Any,
    text: str,
    entities: Optional[Any] = None,
) -> None:
    """Send a text message to destination Telegram channel/group via user_client (with bot_client fallback)."""
    try:
        await user_client.send_message(
            dest_entity,
            text,
            formatting_entities=entities,
            link_preview=False,
        )
    except Exception as user_err:
        try:
            dest_peer_id = utils.get_peer_id(dest_entity)
            await bot_client.send_message(
                dest_peer_id,
                text,
                formatting_entities=entities,
                link_preview=False,
            )
        except Exception:
            raise user_err


async def _upload_media_File_to_tg_dest(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    dest_entity: Any,
    file_path: str,
    actual_size: int,
    caption: str,
    entities: Optional[Any],
    attributes: Optional[Any],
    thumb_path: Optional[str],
    ul_progress: Optional[Any] = None,
) -> None:
    """Upload a local media file to destination Telegram channel/group using parallel MTProto upload."""
    for client_idx, client in enumerate((user_client, bot_client)):
        try:
            target = dest_entity if client_idx == 0 else utils.get_peer_id(dest_entity)
            upload_handle: Any = file_path
            if actual_size >= 1024 * 1024:
                try:
                    upload_handle = await fast_upload_file(
                        client,
                        file_path,
                        progress_callback=ul_progress,
                    )
                except FilePartsInvalidError:
                    raise
                except Exception as fast_ul_err:
                    logger.warning(
                        "Parallel TG upload failed (%s), falling back to standard upload",
                        fast_ul_err,
                    )
                    upload_handle = file_path

            await client.send_file(
                target,
                file=upload_handle,
                caption=caption,
                formatting_entities=entities,
                attributes=attributes,
                thumb=thumb_path,
                supports_streaming=True,
                progress_callback=ul_progress if upload_handle == file_path else None,
            )
            return
        except Exception as err:
            if client_idx == 0:
                logger.debug("user_client upload to TG dest failed (%s), trying bot_client", err)
                continue
            raise


async def send_posts_to_telegram(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    event: events.NewMessage.Event,
    posts: List[ParsedPostLink],
    dest_entity: Any,
) -> None:
    """
    Bulk-download (or server-side copy) every message referenced by `posts` and upload
    each file or text post to the destination Telegram channel/group (`dest_entity`).
    """
    chat_id = event.chat_id
    loop = asyncio.get_running_loop()
    total = sum(len(p.msg_ids) for p in posts)
    dest_title = getattr(dest_entity, "title", None) or getattr(dest_entity, "username", None) or str(dest_entity.id)
    dest_peer_id = utils.get_peer_id(dest_entity)

    status_msg = await event.reply(
        f"✈️ **Telegram Channel Upload Started** → **{dest_title}** (`{dest_peer_id}`)\n"
        f"📦 `{total}` message(s) queued. Send `/cancel` anytime to stop."
    )

    ACTIVE_BULK_CHATS.add(chat_id)
    CANCELLED_CHATS.discard(chat_id)

    sent = 0
    skipped: List[str] = []
    failed: List[str] = []
    aborted_reason = ""
    last_sent_link: Optional[str] = None
    last_sent_msg_id: Optional[int] = None
    idx = 0

    try:
        for post in posts:
            if chat_id in CANCELLED_CHATS or aborted_reason:
                break
            try:
                entity = await resolve_post_entity(user_client, post)
            except Exception as e:
                failed.append(
                    f"`{post.chat_identifier}`: cannot access source channel ({type(e).__name__})"
                )
                idx += len(post.msg_ids)
                continue

            src_noforwards = bool(getattr(entity, "noforwards", False))

            for msg_id in post.msg_ids:
                idx += 1
                if chat_id in CANCELLED_CHATS or aborted_reason:
                    break
                prefix = f"**[{idx}/{total}]**"
                item_sent = False
                flood_retries = 0

                while flood_retries < 3 and chat_id not in CANCELLED_CHATS:
                    temp_dir = tempfile.mkdtemp(prefix=f"tg_ul_{msg_id}_", dir=DOWNLOAD_DIR)
                    try:
                        msg = await user_client.get_messages(entity, ids=msg_id)
                        if not msg:
                            skipped.append(f"`#{msg_id}`: not found / deleted")
                            break

                        has_file_media = bool(msg.media) and not isinstance(
                            msg.media, MessageMediaWebPage
                        )

                        if not has_file_media:
                            if TG_SEND_TEXT and msg.message:
                                await _send_text_to_tg_dest(
                                    user_client,
                                    bot_client,
                                    dest_entity,
                                    msg.message,
                                    entities=msg.entities,
                                )
                                sent += 1
                                item_sent = True
                                last_sent_link = format_post_link(post, msg_id)
                                last_sent_msg_id = msg_id
                            else:
                                skipped.append(f"`#{msg_id}`: text-only / empty post (skipped)")
                            break

                        file_obj = getattr(msg, "file", None)
                        file_size = getattr(file_obj, "size", 0) or 0
                        raw_name = (file_obj and file_obj.name) or (
                            f"file_{msg_id}{utils.get_extension(msg.media) or '.mp4'}"
                        )
                        media_caption = (msg.message or "") if TG_SEND_CAPTIONS else ""
                        media_entities = msg.entities if TG_SEND_CAPTIONS else None

                        # Fast path: if source is not content-restricted, copy media directly on Telegram servers
                        if not src_noforwards and not getattr(msg, "noforwards", False):
                            try:
                                await user_client.send_file(
                                    dest_entity,
                                    file=msg.media,
                                    caption=media_caption,
                                    formatting_entities=media_entities,
                                    supports_streaming=True,
                                )
                                sent += 1
                                item_sent = True
                                last_sent_link = format_post_link(post, msg_id)
                                last_sent_msg_id = msg_id
                                break
                            except Exception as direct_err:
                                logger.debug(
                                    "Direct TG media copy fell back to download+upload for #%s: %s",
                                    msg_id,
                                    direct_err,
                                )

                        # Full download + remux + upload path (for noforwards / restricted channels)
                        last_link_str = f"\n🔗 **Last Sent:** {last_sent_link}" if last_sent_link else ""
                        dl_label = f"⬇️ {prefix} **Downloading `{raw_name}` (`#{msg_id}`)...**\n📢 Target: **{dest_title}**"
                        await status_msg.edit(dl_label + last_link_str, link_preview=False)
                        file_path = await download_message_media(
                            user_client,
                            msg,
                            entity,
                            temp_dir,
                            raw_name,
                            make_progress_callback(status_msg, dl_label, loop, footer=last_link_str),
                        )
                        if not file_path or not os.path.exists(file_path):
                            failed.append(f"`#{msg_id}`: download failed")
                            break
                        if chat_id in CANCELLED_CHATS:
                            break

                        actual_size = os.path.getsize(file_path)
                        mime_type = getattr(file_obj, "mime_type", None)
                        is_video = (
                            bool(mime_type and mime_type.startswith("video/"))
                            or file_path.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".avi", ".ts", ".m4v"))
                        )

                        # Remux video with +faststart for instant Telegram streaming
                        if is_video and actual_size <= 2000 * 1024 * 1024:
                            base_stem = os.path.splitext(os.path.basename(file_path))[0]
                            fast_target = os.path.join(temp_dir, f"{base_stem}_fast.mp4")
                            try:
                                remux_proc = await asyncio.create_subprocess_exec(
                                    "ffmpeg", "-y", "-i", file_path, "-c", "copy", "-movflags", "+faststart",
                                    fast_target,
                                    stdout=asyncio.subprocess.DEVNULL,
                                    stderr=asyncio.subprocess.DEVNULL,
                                )
                                await asyncio.wait_for(remux_proc.communicate(), timeout=30.0)
                                if remux_proc.returncode == 0 and os.path.exists(fast_target) and os.path.getsize(fast_target) > 0:
                                    clean_target = os.path.join(temp_dir, f"{base_stem}.mp4")
                                    try:
                                        if file_path != clean_target and os.path.exists(file_path):
                                            os.remove(file_path)
                                        os.replace(fast_target, clean_target)
                                        file_path = clean_target
                                    except OSError:
                                        file_path = fast_target
                                    actual_size = os.path.getsize(file_path)
                            except Exception as remux_err:
                                logger.debug("Faststart remux skipped: %s", remux_err)

                        thumb_path = None
                        try:
                            thumb_path = await user_client.download_media(
                                msg, thumb=-1, file=os.path.join(temp_dir, "thumb.jpg")
                            )
                        except Exception:
                            thumb_path = None

                        if (not thumb_path or not os.path.exists(thumb_path)) and is_video:
                            extracted_thumb = os.path.join(temp_dir, "thumb.jpg")
                            try:
                                extract_proc = await asyncio.create_subprocess_exec(
                                    "ffmpeg", "-y", "-ss", "00:00:01", "-i", file_path,
                                    "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "4",
                                    extracted_thumb,
                                    stdout=asyncio.subprocess.DEVNULL,
                                    stderr=asyncio.subprocess.DEVNULL,
                                )
                                await asyncio.wait_for(extract_proc.communicate(), timeout=8.0)
                                if (
                                    extract_proc.returncode == 0
                                    and os.path.exists(extracted_thumb)
                                    and os.path.getsize(extracted_thumb) > 0
                                ):
                                    thumb_path = extracted_thumb
                            except Exception as extract_err:
                                logger.debug("ffmpeg thumbnail extraction failed: %s", extract_err)

                        file_basename = os.path.basename(file_path)
                        ul_label = (
                            f"⬆️ {prefix} **Uploading `{file_basename}` to Telegram**\n"
                            f"📢 Channel: **{dest_title}**"
                        )
                        await status_msg.edit(ul_label + last_link_str, link_preview=False)
                        ul_progress = make_progress_callback(
                            status_msg, ul_label, loop, footer=last_link_str
                        )
                        doc = getattr(msg, "document", None)
                        attributes = doc.attributes if doc is not None else None

                        await _upload_media_File_to_tg_dest(
                            user_client,
                            bot_client,
                            dest_entity,
                            file_path,
                            actual_size,
                            media_caption,
                            media_entities,
                            attributes,
                            thumb_path,
                            ul_progress=ul_progress,
                        )
                        sent += 1
                        item_sent = True
                        last_sent_link = format_post_link(post, msg_id)
                        last_sent_msg_id = msg_id
                        break

                    except FloodWaitError as e:
                        flood_retries += 1
                        wait_sec = e.seconds + 2
                        last_link_str = f"\n🔗 **Last sent:** {last_sent_link}" if last_sent_link else ""
                        try:
                            await status_msg.edit(
                                f"⚠️ {prefix} **Telegram FloodWait:** cooling down **{wait_sec}s** "
                                f"before retrying `#{msg_id}`..."
                                f"{last_link_str}",
                                link_preview=False,
                            )
                        except Exception:
                            pass
                        await _cancellable_sleep(chat_id, wait_sec)
                    except Exception as e:
                        logger.exception("Telegram channel upload failed for %s #%s", post.raw, msg_id)
                        err_text = f"{type(e).__name__}: {e}"
                        failed.append(f"`#{msg_id}`: {err_text}")
                        if any(
                            k in err_text.lower()
                            for k in ("chatwriteforbidden", "chatadminrequired", "userbannedinchannel")
                        ):
                            aborted_reason = f"No permission to post in **{dest_title}** ({type(e).__name__})"
                        break
                    finally:
                        shutil.rmtree(temp_dir, ignore_errors=True)

                if idx < total and chat_id not in CANCELLED_CHATS and not aborted_reason:
                    if item_sent:
                        if (
                            BULK_COOLDOWN_EVERY > 0
                            and BULK_COOLDOWN_SECONDS > 0
                            and sent % BULK_COOLDOWN_EVERY == 0
                        ):
                            last_link_str = f"\n🔗 **Last sent:** {last_sent_link}" if last_sent_link else ""
                            try:
                                await status_msg.edit(
                                    f"⏳ {prefix} **Cooling down {int(BULK_COOLDOWN_SECONDS)}s** "
                                    f"after `{sent}` uploads to avoid rate limits..."
                                    f"{last_link_str}",
                                    link_preview=False,
                                )
                            except Exception:
                                pass
                            await _cancellable_sleep(chat_id, BULK_COOLDOWN_SECONDS)
                        elif TG_SEND_DELAY_SECONDS > 0:
                            await _cancellable_sleep(chat_id, TG_SEND_DELAY_SECONDS)
                    else:
                        await _cancellable_sleep(chat_id, 0.4)
    finally:
        was_cancelled = chat_id in CANCELLED_CHATS
        ACTIVE_BULK_CHATS.discard(chat_id)
        CANCELLED_CHATS.discard(chat_id)

    if aborted_reason:
        header = f"⚠️ **Telegram upload aborted:** {aborted_reason}"
    elif was_cancelled:
        header = "🛑 **Telegram upload cancelled.**"
    else:
        header = "✅ **Telegram channel upload finished.**"

    lines = [
        header,
        f"📢 Channel: **{dest_title}** (`{dest_peer_id}`)",
        f"📤 Sent: `{sent}` / `{total}`",
    ]
    if last_sent_link:
        lines.append(f"🔗 **Last Sent Link:** {last_sent_link}")
        if was_cancelled and last_sent_msg_id:
            next_link = format_post_link(post, last_sent_msg_id + 1)
            lines.append(f"▶️ **Resume with:** `/tg {next_link}`")
    max_detail = 15
    if skipped:
        lines.append(f"\n⏭️ **Skipped ({len(skipped)}):**")
        lines.extend(f"• {s}" for s in skipped[:max_detail])
        if len(skipped) > max_detail:
            lines.append(f"• …and {len(skipped) - max_detail} more")
    if failed:
        lines.append(f"\n❌ **Failed ({len(failed)}):**")
        lines.extend(f"• {f}" for f in failed[:max_detail])
        if len(failed) > max_detail:
            lines.append(f"• …and {len(failed) - max_detail} more")

    try:
        await status_msg.delete()
    except Exception:
        pass
    await bot_client.send_message(chat_id, "\n".join(lines), reply_to=event.id, link_preview=False)


async def upload_direct_message_to_telegram(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    event: events.NewMessage.Event,
    src_msg: Any,
    dest_entity: Any,
    custom_caption: Optional[str] = None,
) -> None:
    """
    Upload a direct media file or text message from the bot chat (replied message or attached media)
    to the destination Telegram channel/group (`dest_entity`).
    """
    dest_title = getattr(dest_entity, "title", None) or getattr(dest_entity, "username", None) or str(dest_entity.id)
    dest_peer_id = utils.get_peer_id(dest_entity)
    loop = asyncio.get_running_loop()

    has_file_media = bool(getattr(src_msg, "media", None)) and not isinstance(
        src_msg.media, MessageMediaWebPage
    )

    if not has_file_media:
        text = custom_caption if custom_caption is not None else (getattr(src_msg, "message", "") or "")
        if not text.strip():
            await event.reply("⚠️ The selected message has no downloadable file or text to upload.")
            return
        try:
            await _send_text_to_tg_dest(
                user_client,
                bot_client,
                dest_entity,
                text,
                entities=getattr(src_msg, "entities", None) if custom_caption is None else None,
            )
            await event.reply(
                f"✅ **Text post sent to Telegram channel!**\n"
                f"📢 **Channel:** **{dest_title}** (`{dest_peer_id}`)"
            )
        except Exception as e:
            await event.reply(f"❌ Failed to send text to **{dest_title}**: `{type(e).__name__}: {e}`")
        return

    file_obj = getattr(src_msg, "file", None)
    raw_name = (file_obj and file_obj.name) or (
        f"file_{src_msg.id}{utils.get_extension(src_msg.media) or '.mp4'}"
    )
    caption = (
        custom_caption
        if custom_caption is not None
        else ((getattr(src_msg, "message", "") or "") if TG_SEND_CAPTIONS else "")
    )
    entities = getattr(src_msg, "entities", None) if (custom_caption is None and TG_SEND_CAPTIONS) else None

    status_msg = await event.reply(
        f"⬆️ **Uploading `{raw_name}` to **{dest_title}** (`{dest_peer_id}`)...**"
    )
    temp_dir = tempfile.mkdtemp(prefix=f"tg_direct_{src_msg.id}_", dir=DOWNLOAD_DIR)
    try:
        dl_label = f"⬇️ **Downloading `{raw_name}`...**\n📢 Target: **{dest_title}**"
        dl_progress = make_progress_callback(status_msg, dl_label, loop)
        # src_msg is in the bot chat, so download via bot_client
        file_path = await download_message_media(
            bot_client,
            src_msg,
            event.chat_id,
            temp_dir,
            raw_name,
            dl_progress,
        )
        if not file_path or not os.path.exists(file_path):
            await status_msg.edit("❌ Failed to download the media file.")
            return

        actual_size = os.path.getsize(file_path)
        thumb_path = None
        try:
            thumb_path = await bot_client.download_media(
                src_msg, thumb=-1, file=os.path.join(temp_dir, "thumb.jpg")
            )
        except Exception:
            thumb_path = None

        ul_label = f"⬆️ **Uploading `{os.path.basename(file_path)}` to **{dest_title}**...**"
        ul_progress = make_progress_callback(status_msg, ul_label, loop)
        doc = getattr(src_msg, "document", None)
        attributes = doc.attributes if doc is not None else None

        await _upload_media_File_to_tg_dest(
            user_client,
            bot_client,
            dest_entity,
            file_path,
            actual_size,
            caption,
            entities,
            attributes,
            thumb_path,
            ul_progress=ul_progress,
        )
        await status_msg.edit(
            f"✅ **Uploaded to Telegram Channel!**\n"
            f"📢 **Channel:** **{dest_title}** (`{dest_peer_id}`)\n"
            f"📄 **File:** `{os.path.basename(file_path)}` (`{format_bytes(actual_size)}`)"
        )
    except Exception as e:
        logger.exception("Direct Telegram upload failed")
        await status_msg.edit(
            f"❌ **Upload to {dest_title} failed:** `{type(e).__name__}: {e}`"
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def sync_single_message_to_telegram(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    rule: SyncRule,
    msg,
) -> bool:
    """
    Auto-sync a single post `msg` from source Telegram channel to `rule`'s destination Telegram channel.
    Supports both unrestricted channels (instant server-side copy) and noforwards channels
    (parallel download + remux + parallel upload).
    """
    if normalize_channel_id(rule.tg_channel_id) == normalize_channel_id(rule.dest_tg_channel_id):
        logger.warning("AutoSync: Skipping self-sync rule #%d", rule.id)
        return False

    dest_peer = PeerChannel(rule.dest_tg_channel_id) if rule.dest_tg_channel_id else rule.dest_tg_peer_id
    dest_entity: Any = dest_peer
    try:
        dest_entity = await user_client.get_entity(dest_peer)
    except Exception:
        try:
            await user_client.get_dialogs()
            dest_entity = await user_client.get_entity(dest_peer)
        except Exception:
            dest_entity = dest_peer

    has_file_media = bool(getattr(msg, "media", None)) and not isinstance(
        msg.media, MessageMediaWebPage
    )

    # 1. Text-only message
    if not has_file_media:
        if not rule.send_text:
            logger.info(
                "AutoSync [TG]: Skipping text-only msg #%s for rule #%s ('%s') (send_text=False)",
                msg.id,
                rule.id,
                rule.tg_title,
            )
            return False

        text = (getattr(msg, "message", "") or getattr(msg, "raw_text", "") or "").strip()
        if not text:
            return False

        try:
            await _send_text_to_tg_dest(
                user_client,
                bot_client,
                dest_entity,
                text,
                entities=getattr(msg, "entities", None),
            )
            sync_manager.record_synced(rule.id, msg.id)
            logger.info(
                "AutoSync [TG]: Sent text #%s from '%s' to TG '%s'",
                msg.id,
                rule.tg_title,
                rule.dest_title,
            )
            if sync_manager.notify_on_sync and sync_manager.notify_chat_id:
                try:
                    preview = text[:100] + ("..." if len(text) > 100 else "")
                    await bot_client.send_message(
                        sync_manager.notify_chat_id,
                        f"🔄 **Auto-Synced to Telegram Channel**\n"
                        f"📢 **From:** {rule.tg_title}\n"
                        f"✈️ **To:** {rule.dest_title} (`{rule.dest_id_display}`)\n"
                        f"💬 **Text:** _{preview}_",
                        link_preview=False,
                    )
                except Exception:
                    pass
            await asyncio.sleep(TG_SEND_DELAY_SECONDS)
            return True
        except Exception as e:
            logger.exception(
                "AutoSync [TG]: Failed sending text #%s to '%s': %s",
                msg.id,
                rule.dest_title,
                e,
            )
            return False

    # 2. File / Media message
    file_obj = getattr(msg, "file", None)
    file_size = getattr(file_obj, "size", 0) or 0
    raw_name = (file_obj and file_obj.name) or (
        f"file_{msg.id}{utils.get_extension(msg.media) or '.bin'}"
    )
    caption = (getattr(msg, "message", "") or getattr(msg, "raw_text", "") or "") if rule.send_captions else ""
    entities = getattr(msg, "entities", None) if rule.send_captions else None

    src_peer = PeerChannel(rule.tg_channel_id)
    src_entity: Any = src_peer
    try:
        src_entity = await user_client.get_entity(src_peer)
    except Exception:
        src_entity = src_peer

    src_noforwards = bool(getattr(src_entity, "noforwards", False)) or bool(
        getattr(msg, "noforwards", False)
    )

    # Fast server-side media copy when source channel is not content-restricted
    if not src_noforwards:
        try:
            await user_client.send_file(
                dest_entity,
                file=msg.media,
                caption=caption,
                formatting_entities=entities,
                supports_streaming=True,
            )
            sync_manager.record_synced(rule.id, msg.id)
            logger.info(
                "AutoSync [TG]: Instant-copied #%s '%s' from '%s' to TG '%s'",
                msg.id,
                raw_name,
                rule.tg_title,
                rule.dest_title,
            )
            if sync_manager.notify_on_sync and sync_manager.notify_chat_id:
                try:
                    await bot_client.send_message(
                        sync_manager.notify_chat_id,
                        f"🔄 **Auto-Synced to Telegram Channel**\n"
                        f"📢 **From:** {rule.tg_title}\n"
                        f"✈️ **To:** {rule.dest_title} (`{rule.dest_id_display}`)\n"
                        f"📦 **File:** `{raw_name}` ({format_bytes(file_size)})",
                        link_preview=False,
                    )
                except Exception:
                    pass
            await asyncio.sleep(TG_SEND_DELAY_SECONDS)
            return True
        except Exception as direct_err:
            logger.debug(
                "AutoSync [TG]: Direct copy failed for #%s (%s); falling back to download+upload",
                msg.id,
                direct_err,
            )

    temp_dir = tempfile.mkdtemp(prefix=f"tg_autosync_{msg.id}_", dir=DOWNLOAD_DIR)
    try:
        try:
            file_path = await asyncio.wait_for(
                download_message_media(
                    user_client,
                    msg,
                    src_entity,
                    temp_dir,
                    raw_name,
                    progress_callback=None,
                ),
                timeout=600.0,
            )
        except asyncio.TimeoutError:
            logger.error("AutoSync [TG]: Download timed out after 600s for msg #%s (%s)", msg.id, raw_name)
            return False

        if not file_path or not os.path.exists(file_path):
            logger.error("AutoSync [TG]: Failed to download media for msg #%s (%s)", msg.id, raw_name)
            return False

        actual_size = os.path.getsize(file_path)
        mime_type = getattr(file_obj, "mime_type", None)
        is_video = (
            bool(mime_type and mime_type.startswith("video/"))
            or file_path.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".avi", ".ts", ".m4v"))
        )

        if is_video and actual_size <= 2000 * 1024 * 1024:
            base_stem = os.path.splitext(os.path.basename(file_path))[0]
            fast_target = os.path.join(temp_dir, f"{base_stem}_fast.mp4")
            try:
                remux_proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-i", file_path, "-c", "copy", "-movflags", "+faststart",
                    fast_target,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(remux_proc.communicate(), timeout=30.0)
                if remux_proc.returncode == 0 and os.path.exists(fast_target) and os.path.getsize(fast_target) > 0:
                    clean_target = os.path.join(temp_dir, f"{base_stem}.mp4")
                    try:
                        if file_path != clean_target and os.path.exists(file_path):
                            os.remove(file_path)
                        os.replace(fast_target, clean_target)
                        file_path = clean_target
                    except OSError:
                        file_path = fast_target
                    actual_size = os.path.getsize(file_path)
            except Exception as remux_err:
                logger.debug("Faststart remux skipped: %s", remux_err)

        thumb_path = None
        try:
            thumb_path = await user_client.download_media(
                msg, thumb=-1, file=os.path.join(temp_dir, "thumb.jpg")
            )
        except Exception:
            thumb_path = None

        if (not thumb_path or not os.path.exists(thumb_path)) and is_video:
            extracted_thumb = os.path.join(temp_dir, "thumb.jpg")
            try:
                extract_proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-ss", "00:00:01", "-i", file_path,
                    "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "4",
                    extracted_thumb,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(extract_proc.communicate(), timeout=8.0)
                if (
                    extract_proc.returncode == 0
                    and os.path.exists(extracted_thumb)
                    and os.path.getsize(extracted_thumb) > 0
                ):
                    thumb_path = extracted_thumb
            except Exception as extract_err:
                logger.debug("ffmpeg thumbnail extraction failed: %s", extract_err)

        file_name = os.path.basename(file_path)
        doc = getattr(msg, "document", None)
        attributes = doc.attributes if doc is not None else None

        for attempt in range(1, 4):
            try:
                await _upload_media_File_to_tg_dest(
                    user_client,
                    bot_client,
                    dest_entity,
                    file_path,
                    actual_size,
                    caption,
                    entities,
                    attributes,
                    thumb_path,
                )
                sync_manager.record_synced(rule.id, msg.id)
                logger.info(
                    "AutoSync [TG]: Successfully uploaded #%s '%s' to TG '%s'",
                    msg.id,
                    file_name,
                    rule.dest_title,
                )
                if sync_manager.notify_on_sync and sync_manager.notify_chat_id:
                    try:
                        await bot_client.send_message(
                            sync_manager.notify_chat_id,
                            f"🔄 **Auto-Synced to Telegram Channel**\n"
                            f"📢 **From:** {rule.tg_title}\n"
                            f"✈️ **To:** {rule.dest_title} (`{rule.dest_id_display}`)\n"
                            f"📦 **File:** `{file_name}` ({format_bytes(actual_size)})",
                            link_preview=False,
                        )
                    except Exception:
                        pass
                await asyncio.sleep(TG_SEND_DELAY_SECONDS)
                return True
            except FloodWaitError as fwe:
                logger.warning("AutoSync [TG]: FloodWait %ds on attempt %d/3 for #%s", fwe.seconds, attempt, msg.id)
                await asyncio.sleep(fwe.seconds + 2)
            except Exception as err:
                logger.exception("AutoSync [TG]: Upload error (attempt %d/3) for #%s: %s", attempt, msg.id, err)
                if attempt < 3:
                    await asyncio.sleep(5)
                else:
                    return False

        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def sync_single_message_for_rule(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    rule: SyncRule,
    msg,
) -> bool:
    """Dispatch auto-sync of a single message to either Telegram or WhatsApp based on rule.target_type."""
    if rule.is_telegram_target:
        return await sync_single_message_to_telegram(user_client, bot_client, rule, msg)
    return await sync_single_message_to_whatsapp(user_client, bot_client, rule, msg)


async def sync_single_message_to_whatsapp(
    user_client: TelegramClient,
    bot_client: TelegramClient,
    rule: SyncRule,
    msg,
) -> bool:
    """
    Download/extract content from msg and send it to rule's WhatsApp group.
    Returns True if sent, False if skipped/failed.
    """
    has_file_media = bool(getattr(msg, "media", None)) and not isinstance(
        msg.media, MessageMediaWebPage
    )

    # 1. Text-only message
    if not has_file_media:
        if not rule.send_text:
            logger.info(
                "AutoSync: Skipping text-only msg #%s for rule #%s ('%s') (send_text=False)",
                msg.id,
                rule.id,
                rule.tg_title,
            )
            return False

        text = (getattr(msg, "raw_text", "") or getattr(msg, "message", "") or "").strip()
        if not text:
            return False

        try:
            await wa_client.send_text(rule.wa_group_id, text)
            sync_manager.record_synced(rule.id, msg.id)
            logger.info(
                "AutoSync: Sent text #%s from '%s' to WA '%s'",
                msg.id,
                rule.tg_title,
                rule.wa_group_title,
            )
            if sync_manager.notify_on_sync and sync_manager.notify_chat_id:
                try:
                    preview = text[:100] + ("..." if len(text) > 100 else "")
                    await bot_client.send_message(
                        sync_manager.notify_chat_id,
                        f"🔄 **Auto-Synced to WhatsApp**\n"
                        f"📢 **Channel:** {rule.tg_title}\n"
                        f"👥 **Group:** {rule.wa_group_title}\n"
                        f"💬 **Text:** _{preview}_",
                        link_preview=False,
                    )
                except Exception:
                    pass
            await asyncio.sleep(WA_SEND_DELAY_SECONDS)
            return True
        except WhatsAppError as e:
            logger.warning(
                "AutoSync: WhatsApp error sending text #%s to '%s': %s",
                msg.id,
                rule.wa_group_title,
                e,
            )
            return False
        except Exception as e:
            logger.exception(
                "AutoSync: Failed sending text #%s to '%s': %s",
                msg.id,
                rule.wa_group_title,
                e,
            )
            return False

    # 2. File / Media message
    file_obj = getattr(msg, "file", None)
    file_size = getattr(file_obj, "size", 0) or 0
    raw_name = (file_obj and file_obj.name) or (
        f"file_{msg.id}{utils.get_extension(msg.media) or '.bin'}"
    )
    unique_file_id = extract_media_unique_id(msg)
    if unique_file_id and sync_manager.is_wa_file_uploaded(rule.wa_group_id, unique_file_id):
        if rule.last_synced_msg_id is None or msg.id > rule.last_synced_msg_id:
            rule.last_synced_msg_id = msg.id
            sync_manager.save()
        logger.info(
            "AutoSync: Skipping duplicate file #%s '%s' (%s) for WA '%s' (already uploaded)",
            msg.id,
            raw_name,
            unique_file_id,
            rule.wa_group_title,
        )
        return False

    if file_size and file_size > wa_client.WA_MAX_FILE_MB * 1024 * 1024:
        logger.warning(
            "AutoSync: File #%s '%s' (%s) exceeds WhatsApp limit (%d MB). Skipped.",
            msg.id,
            raw_name,
            format_bytes(file_size),
            int(wa_client.WA_MAX_FILE_MB),
        )
        if sync_manager.notify_on_sync and sync_manager.notify_chat_id:
            try:
                await bot_client.send_message(
                    sync_manager.notify_chat_id,
                    f"⚠️ **Auto-Sync Skipped Post `#{msg.id}`** from **{rule.tg_title}**:\n"
                    f"File `{raw_name}` ({format_bytes(file_size)}) exceeds WhatsApp {int(wa_client.WA_MAX_FILE_MB)} MB limit.",
                    link_preview=False,
                )
            except Exception:
                pass
        return False

    temp_dir = tempfile.mkdtemp(prefix=f"wa_autosync_{msg.id}_", dir=DOWNLOAD_DIR)
    try:
        peer = PeerChannel(rule.tg_channel_id)
        entity = None
        try:
            entity = await user_client.get_entity(peer)
        except Exception:
            entity = peer

        try:
            file_path = await asyncio.wait_for(
                download_message_media(
                    user_client,
                    msg,
                    entity,
                    temp_dir,
                    raw_name,
                    progress_callback=None,
                ),
                timeout=600.0,
            )
        except asyncio.TimeoutError:
            logger.error("AutoSync: Download timed out after 600s for msg #%s (%s)", msg.id, raw_name)
            return False

        if not file_path or not os.path.exists(file_path):
            logger.error("AutoSync: Failed to download media for msg #%s (%s)", msg.id, raw_name)
            return False

        actual_size = os.path.getsize(file_path)
        mime_type = getattr(file_obj, "mime_type", None)
        is_video = (
            bool(mime_type and mime_type.startswith("video/"))
            or file_path.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".avi", ".ts", ".m4v"))
        )

        # Video faststart remux for streaming preview
        if is_video and actual_size <= wa_client.WA_VIDEO_MAX_MB * 1024 * 1024:
            base_stem = os.path.splitext(os.path.basename(file_path))[0]
            fast_target = os.path.join(temp_dir, f"{base_stem}_fast.mp4")
            try:
                remux_proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-i", file_path, "-c", "copy", "-movflags", "+faststart",
                    fast_target,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(remux_proc.communicate(), timeout=30.0)
                if remux_proc.returncode == 0 and os.path.exists(fast_target) and os.path.getsize(fast_target) > 0:
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
                    file_path = fast_target
                    actual_size = os.path.getsize(file_path)
                    mime_type = "video/mp4"
            except Exception as remux_err:
                logger.debug("Faststart remux skipped: %s", remux_err)

        send_type = wa_client.choose_send_type(file_path, mime_type)
        if send_type is None:
            logger.warning("AutoSync: File #%s exceeds limit after download. Skipped.", msg.id)
            return False

        # Metadata extraction
        duration = getattr(file_obj, "duration", None)
        width = getattr(file_obj, "width", None)
        height = getattr(file_obj, "height", None)
        if is_video and (not width or not height or not duration):
            try:
                probe_proc = await asyncio.create_subprocess_exec(
                    "ffprobe", "-v", "error", "-select_streams", "v:0",
                    "-show_entries", "stream=width,height,duration",
                    "-of", "csv=p=0", file_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(probe_proc.communicate(), timeout=5.0)
                if probe_proc.returncode == 0 and stdout:
                    parts = stdout.decode("utf-8", "ignore").strip().split(",")
                    if len(parts) >= 2:
                        if not width and parts[0].isdigit():
                            width = int(parts[0])
                        if not height and parts[1].isdigit():
                            height = int(parts[1])
                    if len(parts) >= 3 and not duration:
                        try:
                            duration = float(parts[2])
                        except ValueError:
                            pass
            except Exception as probe_err:
                logger.debug("ffprobe metadata extraction skipped: %s", probe_err)

        # Thumbnail extraction
        thumb_path = None
        try:
            thumb_path = await user_client.download_media(
                msg, thumb=-1, file=os.path.join(temp_dir, "thumb.jpg")
            )
        except Exception:
            thumb_path = None

        if (not thumb_path or not os.path.exists(thumb_path)) and is_video:
            extracted_thumb = os.path.join(temp_dir, "thumb.jpg")
            seek_time = "00:00:01" if (duration and duration >= 2) else "00:00:00"
            try:
                extract_proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-ss", seek_time, "-i", file_path,
                    "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "4",
                    extracted_thumb,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await asyncio.wait_for(extract_proc.communicate(), timeout=8.0)
                if (
                    extract_proc.returncode == 0
                    and os.path.exists(extracted_thumb)
                    and os.path.getsize(extracted_thumb) > 0
                ):
                    thumb_path = extracted_thumb
            except Exception as extract_err:
                logger.debug("ffmpeg thumbnail extraction failed: %s", extract_err)

        file_name = os.path.basename(file_path)
        caption = (getattr(msg, "raw_text", "") or getattr(msg, "message", "") or "") if rule.send_captions else ""

        # Send to WhatsApp with retries
        for attempt in range(1, 4):
            try:
                await wa_client.send_file(
                    rule.wa_group_id,
                    file_path,
                    send_type,
                    caption=caption,
                    file_name=file_name,
                    mime_type=mime_type,
                    thumb_path=thumb_path,
                    seconds=duration,
                    width=width,
                    height=height,
                )
                sync_manager.mark_wa_file_uploaded(rule.wa_group_id, unique_file_id, persist=False)
                sync_manager.record_synced(rule.id, msg.id)
                logger.info(
                    "AutoSync: Successfully uploaded #%s '%s' (%s) to WA '%s'",
                    msg.id,
                    file_name,
                    send_type,
                    rule.wa_group_title,
                )
                if sync_manager.notify_on_sync and sync_manager.notify_chat_id:
                    try:
                        await bot_client.send_message(
                            sync_manager.notify_chat_id,
                            f"🔄 **Auto-Synced to WhatsApp**\n"
                            f"📢 **Channel:** {rule.tg_title}\n"
                            f"👥 **Group:** {rule.wa_group_title}\n"
                            f"📦 **File:** `{file_name}` ({format_bytes(actual_size)})",
                            link_preview=False,
                        )
                    except Exception:
                        pass
                await asyncio.sleep(WA_SEND_DELAY_SECONDS)
                return True
            except WhatsAppError as wa_err:
                logger.warning(
                    "AutoSync: WhatsApp error (attempt %d/3) for #%s: %s",
                    attempt,
                    msg.id,
                    wa_err,
                )
                if attempt < 3:
                    await asyncio.sleep(5)
                else:
                    return False
            except Exception as err:
                logger.exception("AutoSync: Unexpected upload error for #%s: %s", msg.id, err)
                return False

        return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


async def main() -> None:
    subscription_store = SubscriptionStore(os.path.join(BASE_DIR, "payments.sqlite3"))

    async def on_stars_subscription_activated(user_id: int, expires_at: int) -> None:
        logger.info("Stars subscription activated for user %s through %s", user_id, expires_at)

    stars_payments = TelegramStarsPayments(BOT_TOKEN, subscription_store, on_stars_subscription_activated)

    # Initialize User Client (MTProto account that performs channel joins & file downloads)
    user_session = StringSession(STRING_SESSION) if STRING_SESSION else "user_session"
    user_client = TelegramClient(user_session, API_ID, API_HASH, catch_up=True)

    # Initialize Bot Client (BotFather interface you interact with)
    bot_client = TelegramClient("bot_session", API_ID, API_HASH)

    print(
        "Starting User Client (if running for the first time, enter your phone number & login code)..."
    )
    await user_client.start()
    user_me = await user_client.get_me()

    # Automatically authorize the logged-in user account as owner
    OWNER_IDS.add(user_me.id)
    AUTHORIZED_USERS.add(user_me.id)

    print("Starting Bot Client...")
    await bot_client.start(bot_token=BOT_TOKEN)
    bot_me = await bot_client.get_me()

    try:
        await bot_client(
            SetBotCommandsRequest(
                scope=BotCommandScopeDefault(),
                lang_code="",
                commands=[
                    BotCommand(command="start", description="🏠 Open Main Menu & Quick Start Guide"),
                    BotCommand(command="help", description="📖 Step-by-Step Usage Guide"),
                    BotCommand(command="myplan", description="💎 Check Your Plan (Free vs Paid) & Upgrade"),
                    BotCommand(command="terms", description="📜 VIP Plan Terms"),
                    BotCommand(command="paysupport", description="💳 Payment support & refund requests"),
                    BotCommand(command="join", description="🔗 Join Public or Private Channel(s)"),
                    BotCommand(command="bulk", description="📥 Bulk Download a Range of Posts"),
                    BotCommand(command="stream", description="🎬 Get Direct Public Stream Link(s)"),
                    BotCommand(command="tgsync", description="✈️ Auto-Sync Channel to Telegram (1 Free)"),
                    BotCommand(command="sync", description="🔄 Auto-Sync Dashboard (Telegram & WhatsApp)"),
                    BotCommand(command="synclist", description="📋 View & Manage Your Sync Rules"),
                    BotCommand(command="tg", description="📤 Upload Posts/Files to a Telegram Channel"),
                    BotCommand(command="tgchannel", description="⭐ Set Your Default Destination TG Channel"),
                    BotCommand(command="tgchannels", description="📢 List Available Telegram Channels"),
                    BotCommand(command="wa", description="📲 Upload Posts to WhatsApp Group (Paid)"),
                    BotCommand(command="wagroups", description="👥 List WhatsApp Groups (Paid)"),
                    BotCommand(command="wagroup", description="⭐ Show/Set Default WhatsApp Group"),
                    BotCommand(command="cancel", description="🛑 Stop Active Bulk Download/Upload"),
                    BotCommand(command="status", description="📊 Check Bot & Stream Server Status"),
                    BotCommand(command="addpaid", description="👑 Grant Paid/VIP Plan to User ID (Owner)"),
                    BotCommand(command="removepaid", description="👑 Revoke Paid/VIP Plan from User ID (Owner)"),
                    BotCommand(command="paidusers", description="👑 List All Paid/VIP Users (Owner)"),
                    BotCommand(command="refundstars", description="👑 Refund latest Stars payment (Owner)"),
                ],
            )
        )
        logger.info("Registered Telegram bot command menu.")
    except Exception as cmd_err:
        logger.warning("Failed to set bot command menu: %s", cmd_err)

    try:
        await bot_client(
            SetBotInfoRequest(
                lang_code="",
                about=(
                    "📥 Download Restricted Telegram Posts, Stream Videos & Auto-Sync Channels to Telegram or WhatsApp!"
                ),
                description=(
                    "👋 Welcome to the Telegram Channel Downloader, Streamer & Auto-Sync Bot!\n\n"
                    "✨ What you can do:\n"
                    "1️⃣ Join private or public channels (send @username or t.me/+invite link)\n"
                    "2️⃣ Download single or bulk posts — even from forward-restricted channels!\n"
                    "3️⃣ Get instant video stream links for VLC / Browser (/stream)\n"
                    "4️⃣ ✈️ Auto-Sync 1 Telegram channel for FREE (/tgsync)\n"
                    "5️⃣ 💎 Paid VIP: Auto-Sync to WhatsApp Groups & unlimited TG channels!\n\n"
                    "👇 Tap START below to begin!"
                ),
            )
        )
        logger.info("Updated Telegram bot profile description & about text.")
    except Exception as info_err:
        logger.debug("Could not update bot description/about: %s", info_err)

    # Start HTTP Range Stream Server + Cloudflare Quick Tunnel
    public_url = await start_stream_server(user_client)

    # Start the local WhatsApp (Baileys) sidecar used by /wa
    try:
        if await wa_client.start_sidecar():
            logger.info("WhatsApp sidecar ready at %s", wa_client.WA_SIDECAR_URL)
    except Exception as wa_err:
        logger.warning("Could not start WhatsApp sidecar: %s", wa_err)

    logger.info(
        "User account connected: %s (@%s, ID: %s)",
        user_me.first_name,
        user_me.username or "no_username",
        user_me.id,
    )
    logger.info(
        "Bot interface ready: @%s (ID: %s)",
        bot_me.username,
        bot_me.id,
    )
    logger.info("Public Stream Base URL: %s", public_url)
    logger.info("Owner User IDs: %s", sorted(OWNER_IDS))
    logger.info("Authorized User IDs: %s", sorted(AUTHORIZED_USERS))
    logger.info(
        "Forward Log Channel: %s (%s)",
        FORWARD_CHANNEL_NUM_ID or "None",
        FORWARD_CHANNEL_PEER_ID or "None",
    )

    forward_target_entity = None

    async def init_forward_channel(num_id: int, peer_id: int):
        nonlocal forward_target_entity
        if not num_id:
            forward_target_entity = None
            return None

        # 1. Check with user_client
        try:
            ch = await user_client.get_entity(PeerChannel(num_id))
            ch_title = getattr(ch, "title", str(num_id))
            logger.info("Identified forward channel '%s' (%s) via user account", ch_title, peer_id)
            try:
                await user_client(InviteToChannelRequest(ch, [bot_me]))
            except Exception:
                pass
            try:
                rights = ChatAdminRights(
                    post_messages=True,
                    edit_messages=True,
                    delete_messages=True,
                    invite_users=True,
                )
                await user_client(EditAdminRequest(ch, bot_me, rights, rank="Logger Bot"))
                logger.info("Bot @%s ensured admin in forward channel '%s'", bot_me.username, ch_title)
            except Exception as e:
                logger.debug("EditAdminRequest in forward channel: %s", e)
        except Exception as e:
            logger.warning("user_client could not resolve forward channel %s: %s", num_id, e)

        # 2. Resolve for bot_client
        for target in (PeerChannel(num_id), peer_id):
            try:
                forward_target_entity = await bot_client.get_entity(target)
                logger.info("bot_client resolved forward channel target: %s", forward_target_entity.id)
                return forward_target_entity
            except Exception:
                continue
        return None

    if FORWARD_CHANNEL_NUM_ID:
        await init_forward_channel(FORWARD_CHANNEL_NUM_ID, FORWARD_CHANNEL_PEER_ID)

    forward_queue: asyncio.Queue = asyncio.Queue()

    def is_forward_channel_target(target) -> bool:
        if not FORWARD_CHANNEL_NUM_ID:
            return False
        if target is None:
            return False
        if target in (FORWARD_CHANNEL_PEER_ID, FORWARD_CHANNEL_NUM_ID):
            return True
        if str(target).strip() in (str(FORWARD_CHANNEL_PEER_ID), str(FORWARD_CHANNEL_NUM_ID)):
            return True
        ent_id = getattr(target, "id", None) or getattr(target, "channel_id", None)
        if ent_id in (FORWARD_CHANNEL_PEER_ID, FORWARD_CHANNEL_NUM_ID):
            return True
        try:
            if utils.get_peer_id(target) == FORWARD_CHANNEL_PEER_ID:
                return True
        except Exception:
            pass
        return False

    def is_temporary_or_progress(text: str) -> bool:
        if not text:
            return False
        t = text.strip()
        if t.startswith(("🔍 Fetching message", "🔄 Starting a new Cloudflare", "⏳ Leaving ", "🔄 Joining ")):
            return True
        if "⚡" in t and ("MB/s" in t or "KB/s" in t or "B/s" in t):
            return True
        if ("[" in t and "%]" in t) or "█" in t or "░" in t:
            return True
        if t.startswith(("⬇️ ", "⬆️ ")) and ("Downloading " in t or "Uploading " in t):
            return True
        if "Bulk Download Started" in t:
            return True
        if t.startswith(("📲 ", "✈️ ")):
            return True
        return False

    # Intercept bot_client messages, file uploads, and final edits to forward bot replies to the log channel
    _orig_send_message = bot_client.send_message
    _orig_send_file = bot_client.send_file
    _orig_edit_message = bot_client.edit_message

    async def hooked_send_message(entity, *args, **kwargs):
        res = await _orig_send_message(entity, *args, **kwargs)
        if not is_forward_channel_target(entity) and res:
            msg_text = getattr(res, "message", "") or (args[0] if args else kwargs.get("message", ""))
            if not is_temporary_or_progress(str(msg_text)):
                forward_queue.put_nowait(res)
        return res

    async def hooked_send_file(entity, *args, **kwargs):
        res = await _orig_send_file(entity, *args, **kwargs)
        if not is_forward_channel_target(entity) and res:
            if isinstance(res, list):
                for m in res:
                    forward_queue.put_nowait(m)
            else:
                forward_queue.put_nowait(res)
        return res

    async def hooked_edit_message(entity, message, *args, **kwargs):
        res = await _orig_edit_message(entity, message, *args, **kwargs)
        if not is_forward_channel_target(entity) and res:
            msg_text = getattr(res, "message", "") or (args[0] if args else kwargs.get("text", ""))
            if not is_temporary_or_progress(str(msg_text)):
                forward_queue.put_nowait(res)
        return res

    bot_client.send_message = hooked_send_message
    bot_client.send_file = hooked_send_file
    bot_client.edit_message = hooked_edit_message

    async def forward_worker() -> None:
        """Background worker that continuously forwards incoming user messages and bot replies in order."""
        while True:
            msg = await forward_queue.get()
            try:
                target = forward_target_entity
                if target is None and FORWARD_CHANNEL_NUM_ID:
                    target = await init_forward_channel(FORWARD_CHANNEL_NUM_ID, FORWARD_CHANNEL_PEER_ID)

                if target is not None:
                    try:
                        await bot_client.forward_messages(target, msg)
                        sender_label = "bot reply" if getattr(msg, "out", False) else f"user {msg.sender_id}"
                        logger.info(
                            "Forwarded %s (msg #%s) to channel %s",
                            sender_label,
                            msg.id,
                            FORWARD_CHANNEL_PEER_ID,
                        )
                        continue
                    except FloodWaitError as fwe:
                        logger.warning("FloodWait of %ds when forwarding; waiting...", fwe.seconds)
                        await asyncio.sleep(fwe.seconds + 1)
                        try:
                            await bot_client.forward_messages(target, msg)
                            continue
                        except Exception as fe2:
                            logger.warning("Retry forward failed: %s", fe2)
                    except Exception as fwd_err:
                        logger.warning(
                            "bot_client native forward failed (%s); sending copy to channel",
                            fwd_err,
                        )

                # Fallback: send copy with sender details via bot_client or user_client
                is_outgoing = getattr(msg, "out", False)
                if is_outgoing:
                    header = "🤖 **Bot Reply:**"
                else:
                    sender = await msg.get_sender() if hasattr(msg, "get_sender") else None
                    first_name = getattr(sender, "first_name", "") or ""
                    last_name = getattr(sender, "last_name", "") or ""
                    full_name = f"{first_name} {last_name}".strip() or "User"
                    username = f"@{sender.username}" if getattr(sender, "username", None) else "no_username"
                    uid = msg.sender_id
                    header = f"👤 **From:** [{full_name}](tg://user?id={uid}) (`{uid}`, {username})"

                raw_text = getattr(msg, "raw_text", "") or getattr(msg, "message", "") or ""
                content = f"{header}\n\n{raw_text}" if raw_text else header

                client_to_use = bot_client if target is not None else user_client
                dest_peer = target if target is not None else PeerChannel(FORWARD_CHANNEL_NUM_ID)

                try:
                    if getattr(msg, "media", None) and not isinstance(msg.media, MessageMediaWebPage):
                        await client_to_use.send_file(
                            dest_peer,
                            file=msg.media,
                            caption=content,
                        )
                    else:
                        await client_to_use.send_message(
                            dest_peer,
                            content,
                            link_preview=False,
                        )
                    logger.info("Sent message copy to forward channel")
                except Exception as copy_err:
                    logger.error("Failed to forward/copy message to channel %s: %s", FORWARD_CHANNEL_ID_RAW, copy_err)

            except Exception as loop_err:
                logger.exception("Unexpected error in forward_worker: %s", loop_err)
            finally:
                forward_queue.task_done()

    forward_task = asyncio.create_task(forward_worker())

    # ------------------------------------------------------------------
    # Auto-Sync Background Queue & Listener
    # ------------------------------------------------------------------
    global sync_queue
    sync_queue = asyncio.Queue()

    async def sync_worker() -> None:
        """Background worker that continuously pulls new posts from monitored channels and syncs to WhatsApp or Telegram."""
        logger.info("AutoSync background worker started.")
        while True:
            try:
                rule_id, msg, chat_id = await sync_queue.get()
                rule = sync_manager.get_rule(rule_id)
                if rule and rule.enabled:
                    try:
                        await sync_single_message_for_rule(user_client, bot_client, rule, msg)
                    except FloodWaitError as fwe:
                        logger.warning("AutoSync: Telegram FloodWait %ds for rule #%d", fwe.seconds, rule_id)
                        await asyncio.sleep(fwe.seconds + 2)
                    except Exception as loop_err:
                        logger.exception(
                            "AutoSync: Error processing message #%s: %s",
                            getattr(msg, "id", "unknown"),
                            loop_err,
                        )
            except asyncio.CancelledError:
                logger.info("AutoSync worker cancelled.")
                break
            except Exception as q_err:
                logger.exception("Unexpected error in sync_worker: %s", q_err)
            finally:
                sync_queue.task_done()

    sync_task = asyncio.create_task(sync_worker())

    async def sync_poller() -> None:
        """
        Periodic background poller that checks monitored Telegram channels for new posts.
        Ensures auto-sync works reliably even when Telegram MTProto does not push socket updates
        for cold/unfocused channels, and catches messages posted during downtime.
        """
        logger.info("AutoSync periodic poller started.")
        await asyncio.sleep(5)

        # Baseline uninitialized rules on startup
        for rule in sync_manager.get_all_rules():
            if not rule.enabled:
                continue
            try:
                peer = PeerChannel(rule.tg_channel_id)
                entity = await user_client.get_entity(peer)
                if rule.last_synced_msg_id is None:
                    created_dt = None
                    if rule.created_at:
                        try:
                            clean_ts = rule.created_at.replace(" UTC", "").strip()
                            created_dt = datetime.strptime(clean_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        except Exception:
                            created_dt = None

                    recent = await user_client.get_messages(entity, limit=10)
                    if recent:
                        to_sync = []
                        if created_dt:
                            for m in reversed(recent):
                                m_date = getattr(m, "date", None)
                                if m_date and m_date >= created_dt:
                                    to_sync.append(m)

                        if to_sync:
                            logger.info(
                                "AutoSync: Poller found %d posts in '%s' posted since rule creation; queuing",
                                len(to_sync),
                                rule.tg_title,
                            )
                            for m in to_sync:
                                if not sync_manager.is_recently_synced(rule.id, m.id):
                                    sync_manager.mark_recently_synced(rule.id, m.id)
                                    await sync_queue.put((rule.id, m, rule.tg_peer_id))

                        rule.last_synced_msg_id = recent[0].id
                        sync_manager.save()
                        logger.info(
                            "AutoSync: Baseline last_synced_msg_id set to #%d for rule #%d ('%s')",
                            rule.last_synced_msg_id,
                            rule.id,
                            rule.tg_title,
                        )
            except Exception as init_err:
                logger.warning("AutoSync: Poller initial check failed for rule #%d: %s", rule.id, init_err)

        while True:
            try:
                active_rules = [r for r in sync_manager.get_all_rules() if r.enabled]
                for rule in active_rules:
                    try:
                        peer = PeerChannel(rule.tg_channel_id)
                        entity = None
                        try:
                            entity = await user_client.get_entity(peer)
                        except Exception:
                            entity = peer

                        min_id = rule.last_synced_msg_id or 0
                        messages = await user_client.get_messages(
                            entity,
                            limit=10,
                            min_id=min_id,
                        )

                        if messages:
                            for m in reversed(messages):
                                if not m or not m.id:
                                    continue
                                if sync_manager.is_recently_synced(rule.id, m.id):
                                    continue
                                sync_manager.mark_recently_synced(rule.id, m.id)
                                logger.info(
                                    "AutoSync [Poller]: Found new post #%d in channel '%s' -> Queued for %s '%s'",
                                    m.id,
                                    rule.tg_title,
                                    rule.dest_type_label,
                                    rule.dest_title,
                                )
                                await sync_queue.put((rule.id, m, rule.tg_peer_id))

                    except FloodWaitError as fwe:
                        logger.warning("AutoSync poller FloodWait: %ds for rule #%d", fwe.seconds, rule.id)
                        await asyncio.sleep(fwe.seconds + 2)
                    except Exception as rule_err:
                        logger.debug("AutoSync poller check error for rule #%d: %s", rule.id, rule_err)

                    await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                logger.info("AutoSync poller cancelled.")
                break
            except Exception as poller_err:
                logger.exception("Unexpected error in sync_poller loop: %s", poller_err)

            await asyncio.sleep(15)

    sync_poller_task = asyncio.create_task(sync_poller())
    payment_task = asyncio.create_task(stars_payments.run())

    @user_client.on(events.NewMessage)
    async def user_channel_post_listener(event: events.NewMessage.Event) -> None:
        if not event.chat_id:
            return
        rules = [r for r in sync_manager.get_rules_for_channel(event.chat_id) if r.enabled]
        if not rules:
            return

        msg = event.message
        if not msg:
            return

        msg_id = getattr(msg, "id", None)
        if not msg_id:
            return

        for rule in rules:
            if sync_manager.is_recently_synced(rule.id, msg_id):
                continue
            sync_manager.mark_recently_synced(rule.id, msg_id)

            logger.info(
                "AutoSync: Detected new post #%d in channel '%s' (%s) -> Queued for %s '%s'",
                msg_id,
                rule.tg_title,
                rule.tg_peer_id,
                rule.dest_type_label,
                rule.dest_title,
            )
            await sync_queue.put((rule.id, msg, event.chat_id))

    @bot_client.on(events.NewMessage(incoming=True))
    async def incoming_user_message_logger(event: events.NewMessage.Event) -> None:
        if not FORWARD_CHANNEL_NUM_ID:
            return
        if event.out:
            return
        if event.chat_id in (FORWARD_CHANNEL_PEER_ID, FORWARD_CHANNEL_NUM_ID):
            return
        if event.sender_id == bot_me.id:
            return

        if not event.is_private:
            is_directed = False
            if getattr(event.message, "mentioned", False):
                is_directed = True
            elif bot_me.username and f"@{bot_me.username}".lower() in (event.raw_text or "").lower():
                is_directed = True
            elif event.is_reply:
                reply = await event.get_reply_message()
                if reply and reply.sender_id == bot_me.id:
                    is_directed = True
            if not is_directed:
                return

        forward_queue.put_nowait(event.message)

    def is_owner(sender_id: Optional[int]) -> bool:
        return bool(sender_id and sender_id in OWNER_IDS)

    def is_paid_user(sender_id: Optional[int]) -> bool:
        return bool(sender_id and (
            sender_id in OWNER_IDS
            or sender_id in AUTHORIZED_USERS
            or subscription_store.is_active(sender_id)
        ))

    def is_authorized(sender_id: Optional[int]) -> bool:
        return bool(sender_id and (PUBLIC_MODE or is_paid_user(sender_id)))

    def get_user_plan_badge(sender_id: Optional[int]) -> str:
        if is_owner(sender_id):
            return "👑 Bot Owner (Unlimited)"
        if is_paid_user(sender_id):
            return "💎 Paid VIP Plan"
        return "🆓 Free Plan"

    def get_owner_contact_text() -> str:
        if OWNER_CONTACT:
            return OWNER_CONTACT if OWNER_CONTACT.startswith("@") else f"@{OWNER_CONTACT}"
        if getattr(user_me, "username", None):
            return f"@{user_me.username}"
        return "the Bot Admin"

    def payment_terms_text() -> str:
        return (
            "📜 VIP Plan Terms\n\n"
            "• Price: 50 Telegram Stars for one calendar month.\n"
            "• This is a one-time purchase; it does not renew automatically.\n"
            "• VIP access starts after Telegram confirms payment and expires one calendar month later.\n"
            "• A purchase made while VIP is active extends access from the current expiry.\n"
            "• VIP includes WhatsApp sync/uploads and multiple Telegram sync rules.\n"
            "• For billing help or a refund request, contact the bot owner using /paysupport. Telegram Support cannot resolve purchases made through this bot."
        )

    def build_paid_upgrade_prompt(
        user_id: Optional[int], reason: str = ""
    ) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        user_tg_used = sync_manager.count_user_tg_rules(uid) if uid else 0
        contact_str = get_owner_contact_text()
        reason_block = f"{reason}\n\n" if reason else ""

        text = (
            f"💎 **Upgrade to Paid VIP Plan**\n\n"
            f"{reason_block}"
            f"👤 **Your Telegram ID:** `{uid}`\n"
            f"🏷️ **Current Plan:** {get_user_plan_badge(uid)}\n\n"
            f"**📊 Plan Comparison:**\n"
            f"• **🆓 Free Plan:**\n"
            f"  ✅ Join Public & Private Channels\n"
            f"  ✅ Single & Bulk Post Downloads (even `noforwards`)\n"
            f"  ✅ Direct Video Stream Links (`/stream`)\n"
            f"  ✅ **{FREE_MAX_TG_SYNC_RULES} Telegram-to-Telegram Auto-Sync Rule** (`{user_tg_used}/{FREE_MAX_TG_SYNC_RULES}` used)\n"
            f"  ❌ WhatsApp Auto-Sync & WhatsApp Bulk Upload\n\n"
            f"• **💎 Paid VIP Plan:**\n"
            f"  ✅ **WhatsApp Group Auto-Sync & Bulk Upload** (`/sync` & `/wa`)\n"
            f"  ✅ **Multiple Telegram-to-Telegram Auto-Sync Rules**\n"
            f"  ✅ Full Priority Access\n\n"
            f"💳 **VIP price:** 50 Telegram Stars for one calendar month. This is a one-time purchase; it does not renew automatically. Read `/terms` before purchasing.\n\n"
            f"👇 Tap the agreement button to continue to checkout, or request manual approval from {contact_str}."
        )
        buttons = [
            [Button.inline("✅ Agree to Terms & Pay 50 Stars", b"payment:agree")],
            [Button.inline("📜 Read Terms", b"payment:terms")],
            [Button.inline("💎 Request Paid Upgrade", b"menu:req_upgrade")],
            [
                Button.inline("✈️ My Auto-Sync Rules", b"sync:list"),
                Button.inline("🏠 Main Menu", b"menu:home"),
            ],
        ]
        return text, buttons

    def build_home_menu_view(user_id: Optional[int]) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        plan_badge = get_user_plan_badge(uid)
        if is_paid_user(uid):
            sync_quota_str = "Unlimited TG & WhatsApp Sync ✅"
        else:
            used_tg = sync_manager.count_user_tg_rules(uid)
            sync_quota_str = f"`{used_tg}/{FREE_MAX_TG_SYNC_RULES}` Free TG Sync used (📲 WhatsApp Sync = 💎 Paid)"

        text = (
            f"👋 **Welcome to @{bot_me.username}!**\n"
            f"_Telegram Channel Downloader, Video Streamer & Auto-Sync Bot_\n\n"
            f"🏷️ **Your Plan:** {plan_badge}\n"
            f"🔄 **Auto-Sync Quota:** {sync_quota_str}\n\n"
            f"🚀 **How to Use in 3 Easy Steps:**\n"
            f"**1️⃣ Join a Channel (if private):**\n"
            f"Send any channel `@username` or private invite link (`https://t.me/+...`) so the bot joins it.\n\n"
            f"**2️⃣ Download or Stream Any Post:**\n"
            f"Paste any post link (`https://t.me/c/123456/10`) or batch range (`https://t.me/c/123456/10-25`) right here!\n\n"
            f"**3️⃣ Auto-Sync Channels (`/tgsync` & `/sync`):**\n"
            f"Automatically copy new posts from any channel to your **Telegram channel** (`1 Free`) or **WhatsApp group** (`💎 Paid`).\n\n"
            f"👇 _Tap any button below for step-by-step guides & controls:_"
        )

        buttons: List[List[Button]] = [
            [
                Button.inline("📥 Download & Bulk Guide", b"menu:dl_guide"),
                Button.inline("🔗 How to Join Channels", b"menu:join_guide"),
            ],
            [
                Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
                Button.inline("📤 Upload (/tg & /wa)", b"menu:upload_guide"),
            ],
            [
                Button.inline("🎬 Video Stream Guide", b"menu:stream_guide"),
                Button.inline("📖 All Commands", b"menu:commands"),
            ],
        ]
        if is_owner(uid):
            buttons.append(
                [
                    Button.inline("💎 My Plan Info", b"menu:plan"),
                    Button.inline("👑 Owner Admin Panel", b"menu:owner"),
                ]
            )
        elif is_paid_user(uid):
            buttons.append([Button.inline("💎 My Paid VIP Plan", b"menu:plan")])
        else:
            buttons.append([Button.inline("💎 Upgrade to Paid Plan (WhatsApp & Multi-Sync)", b"menu:plan")])
        return text, buttons

    def build_dl_guide_view() -> Tuple[str, List[List[Button]]]:
        text = (
            "📥 **How to Download Single & Bulk Posts**\n\n"
            "You don't even need a command — just **paste the link(s)** directly into this chat!\n\n"
            "**1️⃣ Single Post / File:**\n"
            "`https://t.me/c/4351613423/2`\n"
            "or `https://t.me/durov/123`\n\n"
            "**2️⃣ Bulk Range of Posts (e.g. posts 2 to 50):**\n"
            "`https://t.me/c/4351613423/2-50`\n\n"
            "**3️⃣ Start Link to End Link:**\n"
            "`/bulk https://t.me/c/4351613423/2 https://t.me/c/4351613423/50`\n\n"
            "**4️⃣ Stop an Active Bulk Job:**\n"
            "Send `/cancel` at any time.\n\n"
            "💡 _Works even on private channels with forwarding/saving disabled (`noforwards`)! Just make sure the bot has joined the private channel first._"
        )
        buttons = [
            [
                Button.inline("🔗 How to Join Channels", b"menu:join_guide"),
                Button.inline("🎬 Stream Guide", b"menu:stream_guide"),
            ],
            [Button.inline("🔙 Back to Main Menu", b"menu:home")],
        ]
        return text, buttons

    def build_join_guide_view() -> Tuple[str, List[List[Button]]]:
        text = (
            "🔗 **How to Join Public & Private Channels**\n\n"
            "Before downloading or syncing from a **private channel**, our connected helper account must be a member of that channel.\n\n"
            "**How to Join:**\n"
            "Simply send the channel link, invite link, or channel ID in this chat:\n"
            "• **Private Invite Link:** `https://t.me/+AbCdEfGhIjKlMnOp`\n"
            "• **Public Channel:** `@channel_username` or `https://t.me/channel_username`\n"
            "• **Channel ID:** `-1001234567890` or `/join -1001234567890`\n"
            "• **Multiple Channels at Once:** `/join https://t.me/+Link1 @channel2 -1001234567890`\n\n"
            "✅ Once joined, you can send any post link (`https://t.me/c/.../...`) or set up `/tgsync` from that channel!"
        )
        buttons = [
            [
                Button.inline("📥 Download Guide", b"menu:dl_guide"),
                Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
            ],
            [Button.inline("🔙 Back to Main Menu", b"menu:home")],
        ]
        return text, buttons

    def build_upload_guide_view(user_id: Optional[int]) -> Tuple[str, List[List[Button]]]:
        paid_note = "✅ Unlocked on your plan" if is_paid_user(user_id) else "🔒 Requires 💎 Paid Plan"
        text = (
            "📤 **Bulk Upload to Telegram (`/tg`) & WhatsApp (`/wa`)**\n\n"
            "**✈️ 1. Upload Posts to Your Telegram Channel (`/tg` — Free):**\n"
            "Copy posts (even from restricted channels) directly to your own Telegram channel:\n"
            "• Set your default channel: `/tgchannel @my_channel`\n"
            "• Bulk upload a range: `/tg https://t.me/c/4351613423/2-30`\n"
            "• Upload to a specific channel: `/tg @my_channel https://t.me/c/4351613423/2-10`\n"
            "• Or reply `/tg @my_channel` to any file/video in this chat!\n"
            "_(⚠️ Make sure you add the bot or helper account as Admin in your destination channel!)_\n\n"
            f"**📲 2. Upload Posts to a WhatsApp Group (`/wa` — {paid_note}):**\n"
            "• Bulk upload to default group: `/wa https://t.me/c/4351613423/2-20`\n"
            "• Upload to a specific group: `/wa My Group Name https://t.me/c/4351613423/2-10`"
        )
        buttons = [
            [
                Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
                Button.inline("💎 Plan & Upgrade", b"menu:plan"),
            ],
            [Button.inline("🔙 Back to Main Menu", b"menu:home")],
        ]
        return text, buttons

    def build_stream_guide_view() -> Tuple[str, List[List[Button]]]:
        text = (
            "🎬 **Instant Video Streaming (`/stream`)**\n\n"
            "Want to watch large videos without waiting for a full download?\n\n"
            "**How to Stream:**\n"
            "• Send `/stream <post_link>`\n"
            "  Example: `/stream https://t.me/c/4351613423/2`\n"
            "• Or stream a range of videos:\n"
            "  `/stream https://t.me/c/4351613423/2-10`\n\n"
            "🌐 The bot generates an instant HTTP Range link that you can open directly in your **Browser**, **VLC**, or **MX Player** with full seeking support!"
        )
        buttons = [
            [
                Button.inline("📥 Download Guide", b"menu:dl_guide"),
                Button.inline("🔙 Back to Main Menu", b"menu:home"),
            ]
        ]
        return text, buttons

    def build_plan_view(user_id: Optional[int]) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        used_tg = sync_manager.count_user_tg_rules(uid)
        if is_owner(uid):
            status_block = (
                "👑 **You are the Bot Owner!**\n"
                "You have unlimited access to all Telegram & WhatsApp syncs and admin settings."
            )
        elif is_paid_user(uid):
            status_block = (
                "💎 **Your Paid VIP Plan is ACTIVE!**\n"
                "You have unlocked **WhatsApp Auto-Sync**, **WhatsApp Bulk Upload (`/wa`)**, and **Multiple Telegram Auto-Sync Rules**!"
            )
        else:
            status_block = (
                f"🆓 **You are currently on the Free Plan.**\n"
                f"• Telegram Auto-Syncs used: `{used_tg}/{FREE_MAX_TG_SYNC_RULES}`\n"
                f"• WhatsApp Auto-Sync & `/wa`: 🔒 **Locked (Paid Only)**"
            )

        text = (
            f"💎 **Account Plan & Features**\n\n"
            f"👤 **Your Telegram ID:** `{uid}`\n"
            f"🏷️ **Plan:** {get_user_plan_badge(uid)}\n\n"
            f"{status_block}\n\n"
            f"**📋 What's Included:**\n"
            f"• **🆓 Free Users:**\n"
            f"  ✅ Unlimited Channel Joins & File/Post Downloads\n"
            f"  ✅ Direct Video Streaming (`/stream`)\n"
            f"  ✅ Bulk Upload to Telegram (`/tg`)\n"
            f"  ✅ **{FREE_MAX_TG_SYNC_RULES} Telegram-to-Telegram Auto-Sync Rule** (`/tgsync`)\n\n"
            f"• **💎 Paid VIP Users:**\n"
            f"  ✅ **WhatsApp Group Auto-Sync** (`/sync add`)\n"
            f"  ✅ **WhatsApp Group Bulk Upload** (`/wa`)\n"
            f"  ✅ **Multiple Telegram Auto-Sync Rules** (`/tgsync`)\n\n"
            f"💳 **VIP:** 50 Telegram Stars for one calendar month; one-time purchase, no automatic renewal. Read `/terms`. For payment help use `/paysupport`."
        )
        buttons: List[List[Button]] = []
        if not is_paid_user(uid):
            buttons.append([Button.inline("✅ Agree to Terms & Pay 50 Stars", b"payment:agree")])
            buttons.append([Button.inline("📜 Read Terms (/terms)", b"payment:terms")])
            buttons.append([Button.inline("💎 Request manual approval", b"menu:req_upgrade")])
        buttons.append(
            [
                Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
                Button.inline("🔙 Back to Main Menu", b"menu:home"),
            ]
        )
        return text, buttons

    def build_commands_view(user_id: Optional[int]) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        owner_cmds = ""
        if is_owner(uid):
            owner_cmds = (
                "\n\n**👑 Owner Admin Commands:**\n"
                "• `/addpaid <user_id>` — Grant Paid VIP access to a user\n"
                "• `/removepaid <user_id>` — Revoke Paid VIP access\n"
                "• `/paidusers` — List all Paid VIP users & Owners\n"
                "• `/walogin` / `/walogout` — Link or unlink WhatsApp device\n"
                "• `/wagroup <name|id>` — Set global default WhatsApp group\n"
                "• `/leave <channel>` — Leave a joined Telegram channel\n"
                "• `/newtunnel` — Restart Cloudflare stream tunnel"
            )

        text = (
            "📖 **Complete Command Reference**\n\n"
            "**📥 Download, Join & Stream (Free):**\n"
            "• `<post_link>` — Download a single post or range (`.../2-50`)\n"
            "• `/bulk <start_link> <end_link>` — Download range of posts\n"
            "• `/stream <post_link>` — Get instant video stream link\n"
            "• `/join <@channel | invite_link>` — Join channel(s)\n"
            "• `/cancel` — Stop active bulk download/upload\n\n"
            "**✈️ Telegram Upload & Auto-Sync (1 Sync Free, Multi = Paid):**\n"
            "• `/tgsync` — Open Telegram Auto-Sync dashboard\n"
            "• `/tgsync add <src_channel> <dest_channel>` — Auto-sync new posts to your TG channel\n"
            "• `/tgchannel <@channel>` — Save your default destination TG channel\n"
            "• `/tg <@channel> <links>` — Bulk copy posts to a TG channel\n\n"
            "**📲 WhatsApp Upload & Auto-Sync (💎 Paid Users Only):**\n"
            "• `/sync` — Open Auto-Sync dashboard (TG & WhatsApp)\n"
            "• `/sync add <src_channel> [wa_group]` — Auto-sync channel to WhatsApp group\n"
            "• `/wa <links_or_range>` — Bulk upload posts to WhatsApp group\n"
            "• `/wagroups` — View available WhatsApp groups\n\n"
            "**⚙️ Account & Info:**\n"
            "• `/start` or `/help` — Open interactive main menu\n"
            "• `/myplan` — Check your plan status & upgrade\n"
            "• `/synclist` — List & manage your active sync rules\n"
            "• `/status` — Check bot status"
            f"{owner_cmds}"
        )
        buttons = [
            [
                Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
                Button.inline("💎 My Plan", b"menu:plan"),
            ],
            [Button.inline("🔙 Back to Main Menu", b"menu:home")],
        ]
        return text, buttons

    def build_owner_panel_view() -> Tuple[str, List[List[Button]]]:
        paid_list = sorted(AUTHORIZED_USERS - OWNER_IDS)
        all_rules = sync_manager.get_all_rules()
        tg_rules = sum(1 for r in all_rules if r.is_telegram_target)
        wa_rules = sum(1 for r in all_rules if not r.is_telegram_target)

        paid_preview = (
            ", ".join(f"`{u}`" for u in paid_list[:15])
            if paid_list
            else "_None yet_"
        )
        if len(paid_list) > 15:
            paid_preview += f" _(+{len(paid_list) - 15} more)_"

        text = (
            "👑 **Bot Owner Admin Panel**\n\n"
            f"• **Public Access Mode:** `{'ON (Free + Paid Tiers)' if PUBLIC_MODE else 'OFF (Paid/Whitelist Only)'}`\n"
            f"• **Free Tier TG Sync Limit:** `{FREE_MAX_TG_SYNC_RULES} rule per Free user`\n"
            f"• **Paid VIP Users ({len(paid_list)}):** {paid_preview}\n"
            f"• **Total Sync Rules:** `{len(all_rules)}` (`✈️ {tg_rules} TG`, `📲 {wa_rules} WA`)\n\n"
            "**⚡ Quick Admin Commands:**\n"
            "• `/addpaid <user_id>` (or reply `/addpaid` to a user) — Grant 💎 Paid VIP plan\n"
            "• `/removepaid <user_id>` — Revoke 💎 Paid VIP plan\n"
            "• `/paidusers` — View full list of Paid users\n"
            "• `/walogin` — Link WhatsApp via QR or phone code"
        )
        buttons = [
            [
                Button.inline("🔄 All Sync Rules", b"sync:list"),
                Button.inline("📊 Sync Engine Status", b"sync:status"),
            ],
            [Button.inline("🔙 Back to Main Menu", b"menu:home")],
        ]
        return text, buttons

    @bot_client.on(events.NewMessage(pattern=r"^/(start|help|menu)(?:@\w+)?(?:\s+.*)?$"))
    async def start_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply(
                f"⛔ **Unauthorized.** Your Telegram ID (`{event.sender_id}`) is not authorized."
            )
            return

        text, buttons = build_home_menu_view(event.sender_id)
        await event.reply(text, buttons=buttons, link_preview=False)

    @bot_client.on(events.NewMessage(pattern=r"^/(myplan|plan|upgrade|vip)(?:@\w+)?$"))
    async def myplan_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply(
                f"⛔ **Unauthorized.** Your Telegram ID (`{event.sender_id}`) is not authorized."
            )
            return

        text, buttons = build_plan_view(event.sender_id)
        await event.reply(text, buttons=buttons, link_preview=False)

    @bot_client.on(events.NewMessage(pattern=r"^/terms(?:@\w+)?$"))
    async def terms_handler(event: events.NewMessage.Event) -> None:
        await event.reply(payment_terms_text())

    @bot_client.on(events.NewMessage(pattern=r"^/paysupport(?:@\w+)?$"))
    async def paysupport_handler(event: events.NewMessage.Event) -> None:
        await event.reply(
            f"For payment support or refund requests, contact {get_owner_contact_text()} and include your Telegram payment receipt. Telegram Support cannot resolve purchases made through this bot."
        )

    @bot_client.on(events.CallbackQuery(pattern=b"^payment:"))
    async def payment_checkout_handler(event: events.CallbackQuery.Event) -> None:
        data = event.data.decode("utf-8")
        if data == "payment:terms":
            await event.answer()
            try:
                await event.edit(
                    payment_terms_text(),
                    buttons=[
                        [Button.inline("✅ I Agree — Continue to Checkout", b"payment:agree")],
                        [Button.inline("🔙 Back to Plan", b"menu:plan")],
                    ],
                )
            except MessageNotModifiedError:
                pass
            return
        if data != "payment:agree":
            await event.answer("Unknown payment action.", alert=True)
            return
        uid = event.sender_id
        if not uid:
            await event.answer("Could not identify your Telegram account.", alert=True)
            return
        if is_paid_user(uid):
            await event.answer("Your VIP plan is already active.", alert=True)
            return
        try:
            await stars_payments.send_invoice(uid)
            await event.answer("Invoice sent. Complete checkout to activate VIP.")
        except Exception as exc:
            logger.exception("Could not create Stars invoice for user %s", uid)
            await event.answer(f"Checkout unavailable: {str(exc)[:180]}", alert=True)

    @bot_client.on(events.CallbackQuery(pattern=r"^menu:"))
    async def menu_callback_handler(event: events.CallbackQuery.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.answer("⛔ Unauthorized.", alert=True)
            return

        data = event.data.decode("utf-8")
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else "home"
        arg = parts[2] if len(parts) > 2 else ""

        if action == "home":
            await event.answer()
            text, buttons = build_home_menu_view(event.sender_id)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "dl_guide":
            await event.answer()
            text, buttons = build_dl_guide_view()
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "join_guide":
            await event.answer()
            text, buttons = build_join_guide_view()
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "upload_guide":
            await event.answer()
            text, buttons = build_upload_guide_view(event.sender_id)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "stream_guide":
            await event.answer()
            text, buttons = build_stream_guide_view()
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "plan":
            await event.answer()
            text, buttons = build_plan_view(event.sender_id)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "commands":
            await event.answer()
            text, buttons = build_commands_view(event.sender_id)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "owner":
            if not is_owner(event.sender_id):
                await event.answer("⛔ Owner only.", alert=True)
                return
            await event.answer()
            text, buttons = build_owner_panel_view()
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "req_upgrade":
            uid = event.sender_id
            if is_paid_user(uid):
                await event.answer("💎 Your Paid VIP Plan is already active!", alert=True)
                return

            sender = await event.get_sender()
            first_name = getattr(sender, "first_name", "") or ""
            last_name = getattr(sender, "last_name", "") or ""
            full_name = f"{first_name} {last_name}".strip() or "User"
            uname = f"@{sender.username}" if getattr(sender, "username", None) else "no_username"

            req_msg = (
                f"💎 **New Paid Plan Upgrade Request!**\n\n"
                f"• 👤 **User:** [{full_name}](tg://user?id={uid}) ({uname})\n"
                f"• 🆔 **User ID:** `{uid}`\n"
                f"• 🔄 **Current Free TG Syncs:** `{sync_manager.count_user_tg_rules(uid)}/{FREE_MAX_TG_SYNC_RULES}`\n\n"
                f"Tap below to instantly activate **💎 Paid VIP Access** for this user:"
            )
            req_buttons = [
                [
                    Button.inline(f"✅ Approve Paid ({uid})", f"menu:approve_paid:{uid}".encode()),
                    Button.inline("❌ Dismiss", b"menu:dismiss"),
                ]
            ]

            notified = False
            for owner_id in sorted(OWNER_IDS):
                try:
                    await _orig_send_message(
                        owner_id, req_msg, buttons=req_buttons, link_preview=False
                    )
                    notified = True
                except Exception as e:
                    logger.debug("Could not DM owner %s for upgrade request: %s", owner_id, e)

            if not notified and forward_target_entity is not None:
                try:
                    await _orig_send_message(
                        forward_target_entity, req_msg, buttons=req_buttons, link_preview=False
                    )
                except Exception:
                    pass

            await event.answer("✅ Upgrade request sent to the bot owner!", alert=True)
            contact_str = get_owner_contact_text()
            confirm_text = (
                f"✅ **Upgrade Request Sent!**\n\n"
                f"• 👤 **Your Telegram ID:** `{uid}`\n"
                f"• 📩 **Contact Admin:** {contact_str}\n\n"
                f"The bot owner has been notified. As soon as your **💎 Paid VIP Plan** is approved, "
                f"you will receive a confirmation message right here!"
            )
            try:
                await event.edit(
                    confirm_text,
                    buttons=[[Button.inline("🔙 Back to Main Menu", b"menu:home")]],
                    link_preview=False,
                )
            except MessageNotModifiedError:
                pass
            return

        if action == "approve_paid" and arg.lstrip("-").isdigit():
            if not is_owner(event.sender_id):
                await event.answer("⛔ Only the bot owner can approve Paid users.", alert=True)
                return
            target_uid = int(arg)
            AUTHORIZED_USERS.add(target_uid)
            save_authorized_users_to_env()
            await event.answer(f"✅ Approved Paid VIP Plan for {target_uid}!", alert=True)
            try:
                await event.edit(
                    f"✅ **Paid VIP Plan Activated for User `{target_uid}`!**\n"
                    f"Approved by Owner `{event.sender_id}`.",
                    buttons=[
                        [
                            Button.inline(
                                f"🗑️ Revoke Paid ({target_uid})",
                                f"menu:revoke_paid:{target_uid}".encode(),
                            )
                        ]
                    ],
                )
            except Exception:
                pass
            try:
                await bot_client.send_message(
                    target_uid,
                    "🎉 **Congratulations! Your 💎 Paid VIP Plan is Now Active!**\n\n"
                    "You now have full access to:\n"
                    "• 📲 **WhatsApp Group Auto-Sync** (`/sync add`)\n"
                    "• 📲 **WhatsApp Group Bulk Upload** (`/wa`)\n"
                    "• ✈️ **Multiple Telegram Channel Auto-Sync Rules** (`/tgsync`)\n\n"
                    "👇 Tap below to open the Auto-Sync Dashboard!",
                    buttons=[
                        [
                            Button.inline("🔄 Open Auto-Sync Dashboard", b"sync:main"),
                            Button.inline("🏠 Main Menu", b"menu:home"),
                        ]
                    ],
                )
            except Exception as e:
                logger.debug("Could not notify user %s of paid approval: %s", target_uid, e)
            return

        if action == "revoke_paid" and arg.lstrip("-").isdigit():
            if not is_owner(event.sender_id):
                await event.answer("⛔ Only the bot owner can revoke Paid users.", alert=True)
                return
            target_uid = int(arg)
            if target_uid in OWNER_IDS:
                await event.answer("⚠️ Cannot revoke an Owner ID.", alert=True)
                return
            AUTHORIZED_USERS.discard(target_uid)
            save_authorized_users_to_env()
            await event.answer(f"🗑️ Revoked Paid plan for {target_uid}.", alert=True)
            try:
                await event.edit(
                    f"🗑️ **Revoked Paid VIP Plan for User `{target_uid}`.**",
                    buttons=[
                        [
                            Button.inline(
                                f"✅ Re-Approve ({target_uid})",
                                f"menu:approve_paid:{target_uid}".encode(),
                            )
                        ]
                    ],
                )
            except Exception:
                pass
            return

        if action == "dismiss":
            if not is_owner(event.sender_id):
                await event.answer("⛔ Owner only.", alert=True)
                return
            await event.answer("Dismissed.")
            try:
                await event.delete()
            except Exception:
                pass
            return

    @bot_client.on(
        events.NewMessage(pattern=r"^/refundstars(?:@\w+)?(?:\s+(\d+)(?:\s+(\S+))?)?$")
    )
    async def refund_stars_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can refund Telegram Stars payments.**")
            return
        user_arg = event.pattern_match.group(1)
        charge_id = event.pattern_match.group(2)
        if not user_arg:
            await event.reply(
                "Usage: `/refundstars <user_id> [charge_id]`\n"
                "Omit charge_id to refund the user's latest unrefunded payment."
            )
            return
        user_id = int(user_arg)
        charge_id = charge_id or subscription_store.get_latest_unrefunded_charge(user_id)
        if not charge_id:
            await event.reply("No unrefunded payment was found for that user.")
            return
        refunded = subscription_store.get_payment_refunded(user_id, charge_id)
        if refunded is None:
            await event.reply("No matching payment record was found for that user and charge ID.")
            return
        if refunded:
            await event.reply("That payment has already been marked as refunded.")
            return
        try:
            await stars_payments.refund_stars(user_id, charge_id)
            expiry = subscription_store.mark_refunded(user_id, charge_id)
        except Exception as exc:
            logger.exception("Stars refund failed for user %s", user_id)
            await event.reply(f"Refund failed: {str(exc)[:300]}")
            return
        try:
            await bot_client.send_message(
                user_id,
                "↩️ Your Telegram Stars payment was refunded. "
                + ("Your remaining VIP access is still active." if expiry and expiry > int(time.time()) else "Your paid VIP access has ended."),
            )
        except Exception as exc:
            logger.warning("Refunded user %s but could not send notice: %s", user_id, exc)
        await event.reply(f"✅ Refunded charge for user `{user_id}`; remaining subscription access was recalculated.")

    @bot_client.on(
        events.NewMessage(pattern=r"^/(addpaid|adduser|auth|allow)(?:@\w+)?(?:\s+(.+))?$")
    )
    async def adduser_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can add Paid VIP users.**")
            return

        arg = (event.pattern_match.group(2) or "").strip()
        target_ids = parse_admin_ids_ordered(arg) if arg else []

        # Also support replying to a forwarded message or user message with /addpaid or /adduser
        if not target_ids and event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg:
                fwd_from = getattr(reply_msg, "fwd_from", None)
                fwd_user_id = getattr(getattr(fwd_from, "from_id", None), "user_id", None)
                candidate = fwd_user_id or reply_msg.sender_id
                if candidate and candidate != bot_me.id:
                    target_ids.append(int(candidate))

        if not target_ids:
            await event.reply(
                "💎 **Add Paid VIP User(s):**\n"
                "Usage: `/addpaid <user_id> [user_id2 ...]`\n"
                "Example: `/addpaid 123456789`\n"
                "_(Or reply `/addpaid` to a user's message)_"
            )
            return

        added: List[int] = []
        already: List[int] = []
        for uid in target_ids:
            if uid in AUTHORIZED_USERS:
                already.append(uid)
            else:
                AUTHORIZED_USERS.add(uid)
                added.append(uid)

        if added:
            save_authorized_users_to_env()
            for uid in added:
                try:
                    await bot_client.send_message(
                        uid,
                        "🎉 **Your account has been upgraded to the 💎 Paid VIP Plan!**\n\n"
                        "You can now use **WhatsApp Auto-Sync (`/sync`)**, **WhatsApp Bulk Upload (`/wa`)**, "
                        "and **Multiple Telegram Auto-Sync Rules (`/tgsync`)**!",
                        buttons=[[Button.inline("🔄 Open Auto-Sync Dashboard", b"sync:main")]],
                    )
                except Exception:
                    pass

        lines: List[str] = []
        if added:
            lines.append(
                "💎 **Activated Paid VIP Plan for user ID(s):** "
                + ", ".join(f"`{uid}`" for uid in added)
            )
        if already:
            lines.append(
                "ℹ️ **Already on Paid VIP Plan:** "
                + ", ".join(f"`{uid}`" for uid in already)
            )
        await event.reply("\n".join(lines))

    @bot_client.on(
        events.NewMessage(
            pattern=r"^/(removepaid|removeuser|deluser|rmuser|unauth)(?:@\w+)?(?:\s+(.+))?$"
        )
    )
    async def removeuser_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can remove Paid VIP users.**")
            return

        arg = (event.pattern_match.group(2) or "").strip()
        target_ids = parse_admin_ids_ordered(arg) if arg else []
        if not target_ids:
            await event.reply(
                "Usage: `/removepaid <user_id> [user_id2 ...]`\n"
                "Example: `/removepaid 123456789`"
            )
            return

        removed: List[int] = []
        skipped_owners: List[int] = []
        not_found: List[int] = []

        for uid in target_ids:
            if uid in OWNER_IDS:
                skipped_owners.append(uid)
            elif uid in AUTHORIZED_USERS:
                AUTHORIZED_USERS.discard(uid)
                removed.append(uid)
            else:
                not_found.append(uid)

        if removed:
            save_authorized_users_to_env()

        lines: List[str] = []
        if removed:
            lines.append(
                "🗑️ **Revoked Paid VIP Plan for user ID(s):** "
                + ", ".join(f"`{uid}`" for uid in removed)
            )
        if skipped_owners:
            lines.append(
                "⚠️ **Cannot remove owner ID(s):** "
                + ", ".join(f"`{uid}`" for uid in skipped_owners)
            )
        if not_found:
            lines.append(
                "ℹ️ **Not in Paid VIP list:** "
                + ", ".join(f"`{uid}`" for uid in not_found)
            )
        await event.reply("\n".join(lines))

    @bot_client.on(
        events.NewMessage(pattern=r"^/(paidusers|users|listusers|admins)(?:@\w+)?$")
    )
    async def listusers_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can view Paid VIP users.**")
            return

        regular_users = sorted(AUTHORIZED_USERS - OWNER_IDS)
        lines = [f"💎 **Paid VIP & Owner Users ({len(AUTHORIZED_USERS)})**\n"]
        lines.append("👑 **Owners (Unlimited):**")
        for uid in sorted(OWNER_IDS):
            lines.append(f"• `{uid}`")
        lines.append("\n💎 **Paid VIP Users (WhatsApp + Multi-TG Sync):**")
        if regular_users:
            for uid in regular_users:
                lines.append(f"• `{uid}`")
        else:
            lines.append("• _None_")
        lines.append(
            f"\n🆓 **Free Users:** Public access enabled (`{FREE_MAX_TG_SYNC_RULES}` TG Sync rule max, no WhatsApp Sync)"
        )
        await event.reply("\n".join(lines))

    @bot_client.on(events.NewMessage(pattern=r"^/(cancel|stop)(?:@\w+)?$"))
    async def cancel_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return
        if event.chat_id in ACTIVE_BULK_CHATS:
            CANCELLED_CHATS.add(event.chat_id)
            await event.reply("🛑 Stopping active bulk download...")
        else:
            await event.reply("ℹ️ No active bulk download in this chat.")

    @bot_client.on(events.NewMessage(pattern=r"^/status(?:@\w+)?$"))
    async def status_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return

        fwd_line = ""
        if is_owner(event.sender_id) and FORWARD_CHANNEL_NUM_ID:
            fwd_line = f"\n• **Forward Log Channel:** `{FORWARD_CHANNEL_ID_RAW}` ({getattr(forward_target_entity, 'title', 'channel')})"

        wa_text, _, _ = await wa_status_text()
        user_tg_def = sync_manager.get_user_tg_channel(
            event.sender_id, TG_UPLOAD_CHANNEL_ID if is_owner(event.sender_id) else ""
        )
        wa_group_line = (
            f"\n• **Default WhatsApp Group:** `{WA_GROUP_ID or 'not set'}`"
            if is_paid_user(event.sender_id)
            else ""
        )
        tg_chan_line = f"\n• **Default TG Upload Channel:** `{user_tg_def or 'not set'}`"

        await event.reply(
            f"✅ **Bot Online**\n"
            f"• **Bot:** @{bot_me.username}\n"
            f"• **Your Plan:** {get_user_plan_badge(event.sender_id)}\n"
            f"• **Public Stream URL:** `{get_public_url()}`"
            f"{fwd_line}\n"
            f"• {wa_text}"
            f"{wa_group_line}"
            f"{tg_chan_line}",
            link_preview=False,
        )

    @bot_client.on(events.NewMessage(pattern=r"^/url(?:@\w+)?$"))
    async def url_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return
        await event.reply(
            f"🌐 **Current Public Stream Server URL:**\n`{get_public_url()}`",
            link_preview=False,
        )

    @bot_client.on(events.NewMessage(pattern=r"^/newtunnel(?:@\w+)?$"))
    async def newtunnel_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can restart the Cloudflare Tunnel.**")
            return
        msg = await event.reply("🔄 Starting a new Cloudflare Quick Tunnel...")
        new_url = await restart_cloudflare_tunnel()
        if new_url:
            await msg.edit(f"✅ **New Public Stream URL Ready:**\n`{new_url}`")
        else:
            await msg.edit("❌ Failed to start Cloudflare Tunnel.")

    @bot_client.on(events.NewMessage(pattern=r"^/leave(?:@\w+)?(?:\s+(.+))?$"))
    async def leave_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can make the helper account leave channels.**")
            return

        arg = event.pattern_match.group(1)
        if not arg:
            await event.reply(
                "Usage: `/leave <@username | https://t.me/channel | channel_id>`"
            )
            return

        status_msg = await event.reply(f"⏳ Leaving `{arg.strip()}`...")
        _, result_text = await leave_single_target(user_client, arg.strip())
        await status_msg.edit(result_text)

    # ------------------------------------------------------------------
    # WhatsApp commands
    # ------------------------------------------------------------------

    async def wa_status_text() -> Tuple[str, bool, bool]:
        """Return (status_text, is_connected, has_qr)."""
        try:
            st = await wa_client.get_status()
        except WhatsAppError as e:
            return (
                f"❌ **WhatsApp sidecar offline:** `{e}`\n"
                "Make sure Node.js is installed and run `npm install` inside `wa_sidecar/`, "
                "then restart the bot.",
                False,
                False,
            )
        conn = st.get("connection")
        me = st.get("me") or {}
        if conn == "open":
            return (
                f"✅ **WhatsApp linked** as `{me.get('name') or ''}` (`{me.get('id', '').split(':')[0]}`)",
                True,
                False,
            )
        return (
            f"⚠️ **WhatsApp not linked** (state: `{conn}`)"
            + (f"\nLast error: `{st.get('lastError')}`" if st.get("lastError") else ""),
            False,
            bool(st.get("hasQr")),
        )

    @bot_client.on(events.NewMessage(pattern=r"^/walogin(?:@\w+)?(?:\s+(.+))?$"))
    async def walogin_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can link WhatsApp.**")
            return

        text, connected, has_qr = await wa_status_text()
        if connected:
            await event.reply(text + "\n\nUse `/walogout` to unlink this device.")
            return

        phone = (event.pattern_match.group(1) or "").strip()
        if phone:
            try:
                code = await wa_client.request_pairing_code(phone)
            except WhatsAppError as e:
                await event.reply(f"❌ Could not get pairing code: `{e}`")
                return
            await event.reply(
                f"🔑 **WhatsApp pairing code:** `{code}`\n\n"
                "On your phone: WhatsApp → **Linked devices** → **Link a device** → "
                "**Link with phone number instead**, then enter the code."
            )
            return

        qr_png = await wa_client.get_qr_png() if has_qr else None
        if not qr_png:
            await event.reply(
                text + "\n\nNo QR code available yet — try again in a few seconds, or use "
                "`/walogin <phone_with_country_code>` to get a pairing code."
            )
            return

        qr_path = os.path.join(DOWNLOAD_DIR, "wa_login_qr.png")
        with open(qr_path, "wb") as f:
            f.write(qr_png)
        try:
            await bot_client.send_file(
                event.chat_id,
                qr_path,
                caption=(
                    "📲 **Scan to link WhatsApp**\n"
                    "WhatsApp → **Linked devices** → **Link a device**.\n"
                    "QR codes expire in ~20s — send `/walogin` again for a fresh one.\n"
                    "Or use `/walogin <phone_with_country_code>` for a pairing code."
                ),
                reply_to=event.id,
            )
        finally:
            try:
                os.remove(qr_path)
            except OSError:
                pass

    @bot_client.on(events.NewMessage(pattern=r"^/walogout(?:@\w+)?$"))
    async def walogout_handler(event: events.NewMessage.Event) -> None:
        if not is_owner(event.sender_id):
            await event.reply("⛔ **Only the bot owner can unlink WhatsApp.**")
            return
        try:
            await wa_client.logout()
            await event.reply("👋 WhatsApp unlinked. Use `/walogin` to link again.")
        except WhatsAppError as e:
            await event.reply(f"❌ Logout failed: `{e}`")

    @bot_client.on(events.NewMessage(pattern=r"^/wagroups(?:@\w+)?(?:\s+(.+))?$"))
    async def wagroups_handler(event: events.NewMessage.Event) -> None:
        if not is_paid_user(event.sender_id):
            text, buttons = build_paid_upgrade_prompt(
                event.sender_id,
                reason="🔒 **WhatsApp Groups & Upload (`/wagroups` / `/wa`)** are available exclusively for **💎 Paid VIP Users**.",
            )
            await event.reply(text, buttons=buttons, link_preview=False)
            return
        query = (event.pattern_match.group(1) or "").strip().lower()
        try:
            groups = await wa_client.list_groups(refresh=True)
        except WhatsAppError as e:
            await event.reply(f"❌ `{e}`\nUse `/walogin` to link WhatsApp first.")
            return
        if query:
            groups = [g for g in groups if query in g.subject.lower()]
        if not groups:
            await event.reply("ℹ️ No WhatsApp groups found.")
            return

        lines = [f"👥 **WhatsApp Groups ({len(groups)})**\n"]
        for g in groups:
            star = " ⭐" if g.id == WA_GROUP_ID else ""
            lines.append(f"• **{g.subject or '(no name)'}**{star} — `{g.id}` ({g.size} members)")
        lines.append("\nSet default: `/wagroup <group_id | name>`")

        # Telegram messages are limited to 4096 chars; send in chunks
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 3900:
                await event.reply(chunk)
                chunk = ""
            chunk += line + "\n"
        if chunk:
            await event.reply(chunk)

    @bot_client.on(events.NewMessage(pattern=r"^/wagroup(?:@\w+)?(?:\s+(.+))?$"))
    async def wagroup_handler(event: events.NewMessage.Event) -> None:
        global WA_GROUP_ID
        if not is_paid_user(event.sender_id):
            text, buttons = build_paid_upgrade_prompt(
                event.sender_id,
                reason="🔒 **WhatsApp Group Settings (`/wagroup`)** are available exclusively for **💎 Paid VIP Users**.",
            )
            await event.reply(text, buttons=buttons, link_preview=False)
            return
        selector = (event.pattern_match.group(1) or "").strip()
        if not selector:
            current = f"`{WA_GROUP_ID}`" if WA_GROUP_ID else "_not set_"
            await event.reply(
                f"👥 **Default WhatsApp group:** {current}\n"
                "Set it with `/wagroup <group_id | name>` (see `/wagroups`)."
            )
            return
        if not is_owner(event.sender_id):
            await event.reply(
                "ℹ️ Only the bot owner can change the global default WhatsApp group.\n"
                "You can specify your group directly in `/wa <group_name> <links>` or `/sync add <channel> <group_name>`!"
            )
            return
        try:
            group = await wa_client.resolve_group(selector)
        except WhatsAppError as e:
            await event.reply(f"❌ {e}")
            return
        WA_GROUP_ID = group.id
        save_env_value("WA_GROUP_ID", group.id)
        await event.reply(f"✅ Default WhatsApp group set to **{group.subject}** (`{group.id}`).")

    @bot_client.on(events.NewMessage(pattern=r"^/(?:wa|whatsapp)(?:@\w+)?(?:\s+([\s\S]+))?$"))
    async def wa_upload_handler(event: events.NewMessage.Event) -> None:
        if not is_paid_user(event.sender_id):
            text, buttons = build_paid_upgrade_prompt(
                event.sender_id,
                reason="🔒 **WhatsApp Bulk Upload (`/wa`)** is available exclusively for **💎 Paid VIP Users**.\n_(💡 Free users can use `/tg` to upload to Telegram or `/tgsync` for 1 Free Telegram Auto-Sync!)_",
            )
            await event.reply(text, buttons=buttons, link_preview=False)
            return

        args = (event.pattern_match.group(1) or "").strip()
        posts = extract_post_links(event.raw_text or "")
        selector = strip_post_links(args)

        # Allow replying to a message that contains the links
        if not posts and event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg and reply_msg.raw_text:
                posts = extract_post_links("/wa " + reply_msg.raw_text)

        if not posts:
            await event.reply(
                "⚠️ **Usage:**\n"
                "• `/wa https://t.me/c/4351613423/2-50`\n"
                "• `/wa https://t.me/c/4351613423/2 https://t.me/c/4351613423/50`\n"
                "• `/wa <group name | group_id> <links>` — upload to a specific group\n"
                "• Reply `/wa` to a message containing links\n\n"
                "Default group: " + (f"`{WA_GROUP_ID}`" if WA_GROUP_ID else "_not set_ (use `/wagroup`)")
            )
            return

        if event.chat_id in ACTIVE_BULK_CHATS:
            await event.reply("⏳ A bulk job is already running in this chat. Use `/cancel` first.")
            return

        target = selector or WA_GROUP_ID
        if not target:
            await event.reply(
                "⚠️ No WhatsApp group given and no default set.\n"
                "Use `/wagroups` to list groups, then `/wagroup <group_id | name>`."
            )
            return

        try:
            group = await wa_client.resolve_group(target)
        except WhatsAppError as e:
            text, connected, _ = await wa_status_text()
            await event.reply(f"❌ {e}" + ("" if connected else f"\n\n{text}\nUse `/walogin` to link."))
            return

        await send_posts_to_whatsapp(user_client, bot_client, event, posts, group)

    # ------------------------------------------------------------------
    # Telegram Channel Upload commands (/tg, /tgchannels, /tgchannel)
    # ------------------------------------------------------------------

    @bot_client.on(events.NewMessage(pattern=r"^/(?:tgchannels|tgchats)(?:@\w+)?(?:\s+(.+))?$"))
    async def tgchannels_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return

        user_def = sync_manager.get_user_tg_channel(
            event.sender_id, TG_UPLOAD_CHANNEL_ID if is_owner(event.sender_id) else ""
        )
        if not is_owner(event.sender_id):
            await event.reply(
                "📢 **Your Destination Telegram Channel Setup**\n\n"
                f"• **Your Current Default Channel:** `{user_def or 'Not set'}`\n\n"
                "**How to connect your Telegram channel:**\n"
                "1️⃣ Send your channel's `@username` or private invite link (`https://t.me/+...`) here so our helper account joins it.\n"
                "2️⃣ Promote the helper account to **Admin** (with _Post Messages_ permission) in your destination channel.\n"
                "3️⃣ Set it as your default channel:\n"
                "   `/tgchannel @your_channel` (or `/tgchannel -100123456789`)\n\n"
                "💡 Once set, you can use `/tgsync add <source_channel>` or `/tg <post_links>`!",
                buttons=[
                    [
                        Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
                        Button.inline("🏠 Main Menu", b"menu:home"),
                    ]
                ],
                link_preview=False,
            )
            return

        query = (event.pattern_match.group(1) or "").strip()
        try:
            channels = await list_user_telegram_channels(user_client, query=query)
        except Exception as e:
            await event.reply(f"❌ Failed to list Telegram channels: `{type(e).__name__}: {e}`")
            return

        if not channels:
            await event.reply("ℹ️ No matching Telegram channels or groups found.")
            return

        norm_default = normalize_channel_id(user_def) if user_def else 0
        lines = [f"📢 **Telegram Channels & Groups ({len(channels)})**\n"]
        for ch in channels:
            is_def = norm_default and normalize_channel_id(ch.id) == norm_default
            if is_def or (
                user_def
                and ch.username
                and ch.username.lower() == user_def.lstrip("@").lower()
            ):
                star = " ⭐ (Default)"
            else:
                star = ""
            role = "👑 " if ch.is_creator else ("🛡️ " if ch.is_admin else "")
            uname_str = f"@{ch.username} | " if ch.username else ""
            post_tag = "" if ch.can_post else " _(read-only)_"
            lines.append(f"• {role}**{ch.title}**{star} — {uname_str}`{ch.peer_id}`{post_tag}")

        lines.append("\nSet default destination: `/tgchannel <@username | channel_id | name>`")

        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 3900:
                await event.reply(chunk, link_preview=False)
                chunk = ""
            chunk += line + "\n"
        if chunk:
            await event.reply(chunk, link_preview=False)

    @bot_client.on(events.NewMessage(pattern=r"^/(?:tgchannel|tgchat)(?:@\w+)?(?:\s+(.+))?$"))
    async def tgchannel_handler(event: events.NewMessage.Event) -> None:
        global TG_UPLOAD_CHANNEL_ID
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return
        selector = (event.pattern_match.group(1) or "").strip()
        user_def = sync_manager.get_user_tg_channel(
            event.sender_id, TG_UPLOAD_CHANNEL_ID if is_owner(event.sender_id) else ""
        )
        if not selector:
            current = f"`{user_def}`" if user_def else "_not set_"
            await event.reply(
                f"📢 **Your Default Telegram Destination Channel:** {current}\n\n"
                "Set it with:\n"
                "`/tgchannel <@username | channel_id | invite_link>`\n\n"
                "_(Make sure the helper account is a member/admin with Post Messages permission in your destination channel!)_"
            )
            return

        entity, err = await resolve_channel_for_sync(user_client, selector, bot_client=bot_client)
        if not entity:
            await event.reply(f"❌ {err}")
            return

        ch_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
        ch_peer_id = utils.get_peer_id(entity)
        ch_uname = getattr(entity, "username", None)
        if event.sender_id:
            sync_manager.set_user_tg_channel(event.sender_id, str(ch_peer_id))
        if is_owner(event.sender_id):
            TG_UPLOAD_CHANNEL_ID = str(ch_peer_id)
            save_env_value("TG_UPLOAD_CHANNEL_ID", TG_UPLOAD_CHANNEL_ID)
        uname_note = f" (`@{ch_uname}`)" if ch_uname else ""
        await event.reply(
            f"✅ Your default Telegram destination channel is now set to **{ch_title}**{uname_note} (`{ch_peer_id}`).\n\n"
            f"You can now run:\n"
            f"• `/tgsync add <source_channel>` — Auto-sync new posts to **{ch_title}**\n"
            f"• `/tg <post_links>` — Bulk upload posts to **{ch_title}**"
        )

    @bot_client.on(
        events.NewMessage(pattern=r"^/(?:tg|tgupload|upload)(?:@\w+)?(?:\s+([\s\S]+))?$")
    )
    async def tg_upload_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return

        args = (event.pattern_match.group(1) or "").strip()
        posts = extract_post_links(event.raw_text or "")
        selector = strip_post_links(args).strip("\"'").strip()

        reply_msg = await event.get_reply_message() if event.is_reply else None
        if not posts and reply_msg and reply_msg.raw_text:
            posts = extract_post_links("/tg " + reply_msg.raw_text)

        has_own_media = bool(getattr(event.message, "media", None)) and not isinstance(
            event.message.media, MessageMediaWebPage
        )
        has_reply_content = bool(
            reply_msg
            and (
                (getattr(reply_msg, "media", None) and not isinstance(reply_msg.media, MessageMediaWebPage))
                or getattr(reply_msg, "message", None)
            )
        )

        user_def = sync_manager.get_user_tg_channel(
            event.sender_id, TG_UPLOAD_CHANNEL_ID if is_owner(event.sender_id) else ""
        )

        if not posts and not has_own_media and not has_reply_content:
            await event.reply(
                "📤 **Telegram Channel Upload (`/tg`) Usage:**\n\n"
                "• `/tg https://t.me/c/4351613423/2-50`\n"
                "• `/tg https://t.me/c/4351613423/2 https://t.me/c/4351613423/50`\n"
                "• `/tg <@channel | channel_id> <links>` — upload to a specific channel\n"
                "• Reply `/tg [@channel]` to any file, video, or message in chat to upload it\n\n"
                "⭐ **Your default destination channel:** "
                + (
                    f"`{user_def}`"
                    if user_def
                    else "_not set_ (set with `/tgchannel @your_channel`)"
                )
            )
            return

        if event.chat_id in ACTIVE_BULK_CHATS:
            await event.reply("⏳ A bulk job is already running in this chat. Use `/cancel` first.")
            return

        target = selector or user_def
        if not target:
            await event.reply(
                "⚠️ No destination Telegram channel specified and no default set.\n\n"
                "Either specify the channel directly:\n"
                "`/tg @your_channel https://t.me/c/4351613423/2-10`\n"
                "Or save your default channel first:\n"
                "`/tgchannel @your_channel`"
            )
            return

        dest_entity, err = await resolve_channel_for_sync(user_client, target, bot_client=bot_client)
        if not dest_entity:
            await event.reply(f"❌ {err}")
            return

        if posts:
            await send_posts_to_telegram(user_client, bot_client, event, posts, dest_entity)
        elif has_own_media:
            await upload_direct_message_to_telegram(
                user_client, bot_client, event, event.message, dest_entity, custom_caption=""
            )
        elif reply_msg:
            await upload_direct_message_to_telegram(
                user_client, bot_client, event, reply_msg, dest_entity
            )

    # ------------------------------------------------------------------
    # Auto-Sync View Builders (Interactive Buttons, User-Scoped)
    # ------------------------------------------------------------------

    async def build_sync_dashboard_view(user_id: Optional[int] = None) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        owner_mode = is_owner(uid)
        paid_mode = is_paid_user(uid)
        user_rules = sync_manager.get_rules_for_user(uid, is_owner=owner_mode)
        active_count = sum(1 for r in user_rules if r.enabled)
        wa_rules_count = sum(1 for r in user_rules if not r.is_telegram_target)
        tg_rules_count = sum(1 for r in user_rules if r.is_telegram_target)
        total_synced = sum(r.synced_count for r in user_rules)
        user_tg_def = sync_manager.get_user_tg_channel(uid, TG_UPLOAD_CHANNEL_ID if owner_mode else "")

        if owner_mode:
            quota_line = f"• **Plan:** 👑 Bot Owner (Viewing all `{len(user_rules)}` rules)\n"
        elif paid_mode:
            quota_line = "• **Plan:** 💎 Paid VIP (Unlimited TG & WhatsApp Sync ✅)\n"
        else:
            used_tg = sync_manager.count_user_tg_rules(uid)
            quota_line = (
                f"• **Plan:** 🆓 Free Plan (`{used_tg}/{FREE_MAX_TG_SYNC_RULES}` Free TG Sync used | 📲 WA Sync = 💎 Paid)\n"
            )

        wa_lines = ""
        if paid_mode:
            _, wa_connected, _ = await wa_status_text()
            wa_lines = (
                f"• **WhatsApp:** {'Connected ✅' if wa_connected else 'Offline ⚠️'}\n"
                + (f"• **Default WA Group:** `{WA_GROUP_ID or 'Not set'}`\n" if owner_mode else "")
            )

        notify_line = (
            f"• **Admin Alerts:** `{'🔔 ON' if sync_manager.notify_on_sync else '🔕 OFF'}`\n"
            if owner_mode
            else ""
        )

        quick_list = ""
        if user_rules:
            quick_lines = []
            for r in user_rules[:5]:
                status_emoji = "🟢" if r.enabled else "⏸️"
                owner_tag = f" _(user `{r.owner_id}`)_" if (owner_mode and r.owner_id and r.owner_id != uid) else ""
                quick_lines.append(
                    f"• `#{r.id}` {status_emoji} **{r.tg_title}** ➔ {r.dest_icon} **{r.dest_title}** (`{r.synced_count}` sent){owner_tag}"
                )
            if len(user_rules) > 5:
                quick_lines.append(f"_...and {len(user_rules) - 5} more (tap Manage Rules below)_")
            quick_list = "\n**📌 Your Active Sync Rules:**\n" + "\n".join(quick_lines) + "\n"
        else:
            quick_list = (
                "\n💡 _You haven't added any Auto-Sync rules yet! Tap **➕ Add TG Sync ✈️** below to get started._\n"
            )

        text = (
            "🔄 **Auto-Sync Dashboard (Telegram & WhatsApp)**\n\n"
            f"{quota_line}"
            f"{wa_lines}"
            f"• **Your Default TG Channel:** `{user_tg_def or 'Not set (use /tgchannel)'}`\n"
            f"• **Your Rules:** `{active_count} active` / `{len(user_rules)} total` (`✈️ {tg_rules_count} TG`, `📲 {wa_rules_count} WA`)\n"
            f"• **Total Synced:** `{total_synced} posts`\n"
            f"{notify_line}"
            f"{quick_list}\n"
            "👇 _Tap a button below to configure auto-sync:_"
        )

        wa_btn_label = "➕ Add WA Sync 📲" if paid_mode else "➕ Add WA Sync 📲 (💎 Paid)"
        buttons: List[List[Button]] = [
            [
                Button.inline(f"📋 Manage My Rules ({len(user_rules)})", b"sync:list"),
            ],
            [
                Button.inline("➕ Add TG Sync ✈️", b"sync:add_tg_help"),
                Button.inline(wa_btn_label, b"sync:add_help"),
            ],
        ]
        if owner_mode:
            buttons.append(
                [
                    Button.inline("📢 TG Channels", b"sync:tgchannels"),
                    Button.inline("👥 WA Groups", b"sync:wagroups"),
                ]
            )
            buttons.append(
                [
                    Button.inline(
                        f"Alerts: {'ON 🔔' if sync_manager.notify_on_sync else 'OFF 🔕'}",
                        b"sync:toggle_notify",
                    ),
                    Button.inline("📊 Live Status", b"sync:status"),
                ]
            )
        elif paid_mode:
            buttons.append(
                [
                    Button.inline("👥 View WA Groups", b"sync:wagroups"),
                    Button.inline("📊 Live Status", b"sync:status"),
                ]
            )
        else:
            buttons.append(
                [
                    Button.inline("💎 Upgrade for WhatsApp & Multi-Sync", b"menu:plan"),
                ]
            )

        buttons.append(
            [
                Button.inline("🔄 Refresh", b"sync:main"),
                Button.inline("🏠 Main Menu", b"menu:home"),
            ]
        )
        return text, buttons

    async def build_sync_rules_list_view(user_id: Optional[int] = None) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        owner_mode = is_owner(uid)
        paid_mode = is_paid_user(uid)
        user_rules = sync_manager.get_rules_for_user(uid, is_owner=owner_mode)
        wa_btn_label = "➕ Add WA Sync 📲" if paid_mode else "➕ Add WA Sync 📲 (💎 Paid)"

        if not user_rules:
            text = (
                "📋 **Your Configured Auto-Sync Channels**\n\n"
                "ℹ️ _You don't have any active auto-sync rules yet._\n\n"
                "• **✈️ Free & Paid Users:** Sync any Telegram channel to your Telegram channel (`/tgsync add <src> <dest>`)\n"
                "• **📲 Paid VIP Users:** Sync any Telegram channel to a WhatsApp group (`/sync add <src> <wa_group>`)"
            )
            buttons = [
                [
                    Button.inline("➕ Add TG Sync ✈️", b"sync:add_tg_help"),
                    Button.inline(wa_btn_label, b"sync:add_help"),
                ],
                [Button.inline("🔙 Back to Sync Dashboard", b"sync:main")],
            ]
            return text, buttons

        text = (
            f"📋 **Your Auto-Sync Rules ({len(user_rules)}):**\n\n"
            "Tap any rule below to view stats, pause/resume, toggle captions, delete, or run a 5-post catchup:"
        )

        buttons = []
        row: List[Button] = []
        for r in user_rules:
            status_icon = "🟢" if r.enabled else "⏸️"
            short_title = r.tg_title[:13] + ("…" if len(r.tg_title) > 13 else "")
            label = f"#{r.id} {status_icon}{r.dest_icon} {short_title}"
            row.append(Button.inline(label, f"sync:rule:{r.id}".encode()))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        buttons.append([
            Button.inline("➕ Add TG Sync ✈️", b"sync:add_tg_help"),
            Button.inline(wa_btn_label, b"sync:add_help"),
        ])
        buttons.append([
            Button.inline("🔙 Back to Sync Dashboard", b"sync:main"),
            Button.inline("🏠 Main Menu", b"menu:home"),
        ])
        return text, buttons

    async def build_sync_rule_detail_view(
        rule_id: int, user_id: Optional[int] = None
    ) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        rule = sync_manager.get_rule(rule_id)
        if not rule or not sync_manager.can_user_access_rule(rule, uid, is_owner=is_owner(uid)):
            text = f"❌ Rule `#{rule_id}` not found or you do not have permission to manage it."
            buttons = [[Button.inline("🔙 Back to Rules", b"sync:list")]]
            return text, buttons

        status_badge = "🟢 ACTIVE" if rule.enabled else "⏸️ PAUSED"
        tg_display = f"@{rule.tg_username}" if rule.tg_username else f"`{rule.tg_peer_id}`"
        last_time = rule.last_synced_at or "Never"
        owner_line = (
            f"• 👤 **Created By User ID:** `{rule.owner_id or 'Owner'}`\n"
            if is_owner(uid)
            else ""
        )

        text = (
            f"⚙️ **Auto-Sync Rule #{rule.id}** — {status_badge}\n\n"
            f"• 📢 **Source Channel:** {rule.tg_title} ({tg_display})\n"
            f"• {rule.dest_icon} **Destination ({rule.dest_type_label}):** {rule.dest_title} (`{rule.dest_id_display}`)\n"
            f"{owner_line}"
            f"• 📊 **Posts Synced:** `{rule.synced_count}`\n"
            f"• 🕒 **Last Synced:** `{last_time}`\n"
            f"• 💬 **Text Messages:** `{'Enabled ✅' if rule.send_text else 'Disabled ❌'}`\n"
            f"• 🏷️ **Media Captions:** `{'Enabled ✅' if rule.send_captions else 'Disabled ❌'}`\n\n"
            "👇 _Tap an action below to configure this rule:_"
        )

        pause_resume_btn = (
            Button.inline("⏸️ Pause Sync", f"sync:pause:{rule.id}".encode())
            if rule.enabled
            else Button.inline("▶️ Resume Sync", f"sync:resume:{rule.id}".encode())
        )
        catchup_btn = Button.inline("⚡ Catchup (5 Posts)", f"sync:catchup:{rule.id}".encode())
        text_btn = Button.inline(
            f"💬 Text: {'ON' if rule.send_text else 'OFF'}",
            f"sync:text:{rule.id}".encode(),
        )
        caps_btn = Button.inline(
            f"🏷️ Captions: {'ON' if rule.send_captions else 'OFF'}",
            f"sync:cap:{rule.id}".encode(),
        )
        del_btn = Button.inline("🗑️ Delete Rule", f"sync:del_ask:{rule.id}".encode())
        back_btn = Button.inline("🔙 Rules List", b"sync:list")
        main_btn = Button.inline("🔄 Sync Menu", b"sync:main")

        buttons = [
            [pause_resume_btn, catchup_btn],
            [text_btn, caps_btn],
            [del_btn],
            [back_btn, main_btn],
        ]
        return text, buttons

    async def build_sync_delete_confirm_view(
        rule_id: int, user_id: Optional[int] = None
    ) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        rule = sync_manager.get_rule(rule_id)
        if not rule or not sync_manager.can_user_access_rule(rule, uid, is_owner=is_owner(uid)):
            text = f"❌ Rule `#{rule_id}` not found."
            buttons = [[Button.inline("🔙 Back to Rules", b"sync:list")]]
            return text, buttons

        text = (
            f"⚠️ **Are you sure you want to delete Rule #{rule.id}?**\n\n"
            f"• 📢 **Source:** {rule.tg_title}\n"
            f"• {rule.dest_icon} **Destination ({rule.dest_type_label}):** {rule.dest_title}\n\n"
            "This will permanently stop auto-syncing this channel."
        )
        buttons = [
            [
                Button.inline("🗑️ Yes, Delete", f"sync:del_confirm:{rule.id}".encode()),
                Button.inline("❌ Cancel", f"sync:rule:{rule.id}".encode()),
            ]
        ]
        return text, buttons

    async def build_sync_wagroups_view(refresh: bool = False) -> Tuple[str, List[List[Button]]]:
        try:
            groups = await wa_client.list_groups(refresh=refresh)
        except WhatsAppError as e:
            text = f"❌ **Could not list WhatsApp groups:** `{e}`\n\nUse `/walogin` to link WhatsApp first."
            buttons = [[Button.inline("🔙 Back", b"sync:main")]]
            return text, buttons

        if not groups:
            text = "ℹ️ No WhatsApp groups found for the linked account."
            buttons = [[Button.inline("🔙 Back", b"sync:main")]]
            return text, buttons

        lines = [f"👥 **WhatsApp Groups ({len(groups)} total):**\n"]
        for idx, g in enumerate(groups[:10], start=1):
            subj = g.subject or "_No Subject_"
            is_def = " ⭐ (Default)" if g.id == WA_GROUP_ID else ""
            lines.append(f"{idx}. **{subj}**{is_def}\n   ID: `{g.id}` | `{g.size} members`")

        if len(groups) > 10:
            lines.append(f"\n_...and {len(groups) - 10} more groups._")

        lines.append(f"\n💡 Current default group: `{WA_GROUP_ID or 'Not set'}`")
        lines.append("Use `/sync add <tg_channel> <group_name_or_id>` to sync to any group above.")

        buttons = [
            [
                Button.inline("🔄 Force Refresh", b"sync:wagroups_ref"),
                Button.inline("🔙 Back to Sync Menu", b"sync:main"),
            ]
        ]
        return "\n".join(lines), buttons

    async def build_sync_tgchannels_view() -> Tuple[str, List[List[Button]]]:
        try:
            channels = await list_user_telegram_channels(user_client)
        except Exception as e:
            text = f"❌ **Could not list Telegram channels:** `{e}`"
            buttons = [[Button.inline("🔙 Back", b"sync:main")]]
            return text, buttons

        if not channels:
            text = "ℹ️ No Telegram channels or groups found."
            buttons = [[Button.inline("🔙 Back", b"sync:main")]]
            return text, buttons

        norm_default = normalize_channel_id(TG_UPLOAD_CHANNEL_ID) if TG_UPLOAD_CHANNEL_ID else 0
        lines = [f"📢 **Telegram Channels & Groups ({len(channels)} total):**\n"]
        writable_top: List[TGChannelInfo] = []
        for idx, ch in enumerate(channels[:10], start=1):
            is_def = norm_default and normalize_channel_id(ch.id) == norm_default
            def_badge = " ⭐ (Default)" if is_def else ""
            role = "👑 " if ch.is_creator else ("🛡️ " if ch.is_admin else "")
            uname_str = f"@{ch.username} | " if ch.username else ""
            lines.append(f"{idx}. {role}**{ch.title}**{def_badge}\n   {uname_str}ID: `{ch.peer_id}`")
            if ch.can_post and len(writable_top) < 6:
                writable_top.append(ch)

        if len(channels) > 10:
            lines.append(f"\n_...and {len(channels) - 10} more (use `/tgchannels` to see all)._")

        lines.append(f"\n💡 Current default TG channel: `{TG_UPLOAD_CHANNEL_ID or 'Not set'}`")
        lines.append("Tap a channel button below to set it as default, or use `/tgchannel <id_or_name>`.")

        buttons: List[List[Button]] = []
        row: List[Button] = []
        for ch in writable_top:
            is_def = norm_default and normalize_channel_id(ch.id) == norm_default
            short_t = ch.title[:16] + ("…" if len(ch.title) > 16 else "")
            prefix = "⭐ " if is_def else "📢 "
            row.append(
                Button.inline(f"{prefix}{short_t}", f"sync:set_tgdef:{ch.peer_id}".encode())
            )
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        buttons.append(
            [
                Button.inline("🔄 Refresh", b"sync:tgchannels"),
                Button.inline("🔙 Back to Sync Menu", b"sync:main"),
            ]
        )
        return "\n".join(lines), buttons

    async def build_sync_status_view(user_id: Optional[int] = None) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        owner_mode = is_owner(uid)
        _, wa_connected, _ = await wa_status_text()
        user_rules = sync_manager.get_rules_for_user(uid, is_owner=owner_mode)
        active_count = sum(1 for r in user_rules if r.enabled)
        tg_rules_count = sum(1 for r in user_rules if r.is_telegram_target)
        wa_rules_count = sum(1 for r in user_rules if not r.is_telegram_target)
        total_synced = sum(r.synced_count for r in user_rules)
        q_size = sync_queue.qsize() if sync_queue else 0

        text = (
            "📊 **Auto-Sync Engine Live Status**\n\n"
            f"• **Your Plan:** {get_user_plan_badge(uid)}\n"
            f"• **Queue Pending:** `{q_size} messages waiting`\n"
            f"• **Active Rules:** `{active_count} of {len(user_rules)}` (`✈️ {tg_rules_count} TG`, `📲 {wa_rules_count} WA`)\n"
            f"• **Total Posts Synced:** `{total_synced}`\n"
            f"• **WhatsApp Sidecar:** {'Connected ✅' if wa_connected else 'Offline ⚠️'}\n"
            "⚡ Everything is monitored in real-time."
        )
        buttons = [
            [
                Button.inline("🔄 Refresh Status", b"sync:status"),
                Button.inline("🔙 Back to Sync Menu", b"sync:main"),
            ]
        ]
        return text, buttons

    def build_sync_add_help_view(user_id: Optional[int] = None) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        if not is_paid_user(uid):
            return build_paid_upgrade_prompt(
                uid,
                reason=(
                    "🔒 **Telegram ➔ WhatsApp Auto-Sync is a 💎 Paid VIP Feature!**\n\n"
                    "Only **Paid VIP Users** can auto-sync channels to WhatsApp groups.\n"
                    "💡 _Free users can auto-sync **1 Telegram channel to another Telegram channel** — tap **➕ Add TG Sync ✈️** or upgrade below!_"
                ),
            )

        text = (
            "📲 **How to Add Telegram ➔ WhatsApp Auto-Sync (💎 Paid VIP)**\n\n"
            "Send the `/sync add` command in this chat:\n"
            "`/sync add <telegram_channel> [whatsapp_group]`\n\n"
            "**Examples:**\n"
            "• `/sync add @durov`\n"
            "  _(uses default WhatsApp group)_\n\n"
            "• `/sync add https://t.me/durov \"My Updates Group\"`\n"
            "  _(syncs to a specific group by name)_\n\n"
            "• `/sync add https://t.me/+InviteHash 1203630xxxx@g.us`\n"
            "  _(joins private channel & syncs to group JID)_\n\n"
            "💡 _Want to sync to a Telegram channel instead? Tap 'Add TG Sync ✈️' below!_"
        )
        buttons = [
            [
                Button.inline("👥 View WA Groups", b"sync:wagroups"),
                Button.inline("➕ Add TG Sync ✈️", b"sync:add_tg_help"),
            ],
            [Button.inline("🔙 Back to Sync Menu", b"sync:main")],
        ]
        return text, buttons

    def build_sync_add_tg_help_view(user_id: Optional[int] = None) -> Tuple[str, List[List[Button]]]:
        uid = user_id or 0
        user_def = sync_manager.get_user_tg_channel(uid, TG_UPLOAD_CHANNEL_ID if is_owner(uid) else "")
        if is_paid_user(uid):
            tier_note = "💎 **Your Plan:** Paid VIP (Unlimited Telegram Auto-Sync rules ✅)"
        else:
            used_tg = sync_manager.count_user_tg_rules(uid)
            tier_note = f"🆓 **Your Plan:** Free Plan (`{used_tg}/{FREE_MAX_TG_SYNC_RULES}` Telegram Auto-Sync rule used)"

        text = (
            "✈️ **How to Add Telegram ➔ Telegram Channel Auto-Sync**\n\n"
            f"{tier_note}\n\n"
            "**Step 1:** Make sure our helper account has joined both the source channel and your destination channel (and has **Post Messages** admin permission in your destination channel).\n\n"
            "**Step 2:** Send `/tgsync add` in this chat:\n"
            "`/tgsync add <source_tg_channel> <dest_tg_channel>`\n\n"
            "**Examples:**\n"
            "• `/tgsync add @source_channel @my_target_channel`\n"
            "  _(auto-syncs new posts from `@source_channel` to `@my_target_channel`)_\n\n"
            "• `/tgsync add https://t.me/+InviteHash -1001234567890`\n"
            "  _(joins private channel & syncs to destination channel ID)_\n\n"
            f"⭐ **Your Default Destination TG Channel:** `{user_def or 'Not set (use /tgchannel @channel)'}`"
        )
        buttons: List[List[Button]] = [
            [
                Button.inline("📋 My Sync Rules", b"sync:list"),
                Button.inline("➕ Add WA Sync 📲", b"sync:add_help"),
            ],
            [Button.inline("🔙 Back to Sync Menu", b"sync:main")],
        ]
        return text, buttons

    # ------------------------------------------------------------------
    # Auto-Sync Commands & Callbacks (/sync, /tgsync, /synclist, /unsync)
    # ------------------------------------------------------------------

    @bot_client.on(events.CallbackQuery(pattern=r"^sync:"))
    async def sync_callback_handler(event: events.CallbackQuery.Event) -> None:
        global TG_UPLOAD_CHANNEL_ID
        if not is_authorized(event.sender_id):
            await event.answer("⛔ Unauthorized.", alert=True)
            return

        uid = event.sender_id
        owner_mode = is_owner(uid)
        data = event.data.decode("utf-8")
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        arg = parts[2] if len(parts) > 2 else ""

        if action == "main":
            await event.answer()
            text, buttons = await build_sync_dashboard_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "list":
            await event.answer()
            text, buttons = await build_sync_rules_list_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "add_help":
            await event.answer()
            text, buttons = build_sync_add_help_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "add_tg_help":
            await event.answer()
            text, buttons = build_sync_add_tg_help_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "tgchannels":
            if not owner_mode:
                await event.answer("Use /tgchannel @your_channel to set your destination channel!", alert=True)
                return
            await event.answer("Loading Telegram channels...")
            text, buttons = await build_sync_tgchannels_view()
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "set_tgdef" and arg:
            if uid:
                sync_manager.set_user_tg_channel(uid, arg)
            if owner_mode:
                TG_UPLOAD_CHANNEL_ID = arg
                save_env_value("TG_UPLOAD_CHANNEL_ID", TG_UPLOAD_CHANNEL_ID)
            await event.answer(f"⭐ Default TG channel set to {arg}")
            text, buttons = await build_sync_tgchannels_view()
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "toggle_notify":
            if not owner_mode:
                await event.answer("⛔ Owner only.", alert=True)
                return
            sync_manager.notify_on_sync = not sync_manager.notify_on_sync
            if sync_manager.notify_on_sync:
                sync_manager.notify_chat_id = event.chat_id
            sync_manager.save()
            status_str = "ON 🔔" if sync_manager.notify_on_sync else "OFF 🔕"
            await event.answer(f"Admin notifications: {status_str}")
            text, buttons = await build_sync_dashboard_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "status":
            await event.answer()
            text, buttons = await build_sync_status_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action in ("wagroups", "wagroups_ref"):
            if not is_paid_user(uid):
                await event.answer("💎 Paid VIP Plan required for WhatsApp features.", alert=True)
                text, buttons = build_paid_upgrade_prompt(
                    uid,
                    reason="🔒 **WhatsApp Groups & Auto-Sync** are available exclusively for **💎 Paid VIP Users**.",
                )
                try:
                    await event.edit(text, buttons=buttons, link_preview=False)
                except MessageNotModifiedError:
                    pass
                return
            await event.answer("Loading WhatsApp groups...")
            text, buttons = await build_sync_wagroups_view(refresh=(action == "wagroups_ref"))
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "rule" and arg.isdigit():
            await event.answer()
            text, buttons = await build_sync_rule_detail_view(int(arg), uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "pause" and arg.isdigit():
            rule = sync_manager.get_rule(int(arg))
            if not sync_manager.can_user_access_rule(rule, uid or 0, is_owner=owner_mode):
                await event.answer("⛔ You can only manage your own sync rules.", alert=True)
                return
            rule = sync_manager.toggle_rule(int(arg), enabled=False)
            if rule:
                await event.answer(f"⏸️ Paused {rule.tg_title}")
            text, buttons = await build_sync_rule_detail_view(int(arg), uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "resume" and arg.isdigit():
            rule = sync_manager.get_rule(int(arg))
            if not sync_manager.can_user_access_rule(rule, uid or 0, is_owner=owner_mode):
                await event.answer("⛔ You can only manage your own sync rules.", alert=True)
                return
            rule = sync_manager.toggle_rule(int(arg), enabled=True)
            if rule:
                await event.answer(f"▶️ Resumed {rule.tg_title}")
            text, buttons = await build_sync_rule_detail_view(int(arg), uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "text" and arg.isdigit():
            rule = sync_manager.get_rule(int(arg))
            if not sync_manager.can_user_access_rule(rule, uid or 0, is_owner=owner_mode):
                await event.answer("⛔ You can only manage your own sync rules.", alert=True)
                return
            if rule:
                sync_manager.update_rule(rule.id, send_text=not rule.send_text)
                await event.answer(f"Text sync: {'ON' if rule.send_text else 'OFF'}")
            text, buttons = await build_sync_rule_detail_view(int(arg), uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "cap" and arg.isdigit():
            rule = sync_manager.get_rule(int(arg))
            if not sync_manager.can_user_access_rule(rule, uid or 0, is_owner=owner_mode):
                await event.answer("⛔ You can only manage your own sync rules.", alert=True)
                return
            if rule:
                sync_manager.update_rule(rule.id, send_captions=not rule.send_captions)
                await event.answer(f"Captions: {'ON' if rule.send_captions else 'OFF'}")
            text, buttons = await build_sync_rule_detail_view(int(arg), uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "del_ask" and arg.isdigit():
            await event.answer()
            text, buttons = await build_sync_delete_confirm_view(int(arg), uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "del_confirm" and arg.isdigit():
            rule = sync_manager.get_rule(int(arg))
            if not sync_manager.can_user_access_rule(rule, uid or 0, is_owner=owner_mode):
                await event.answer("⛔ You can only delete your own sync rules.", alert=True)
                return
            rule = sync_manager.remove_rule(int(arg))
            title = rule.tg_title if rule else f"#{arg}"
            await event.answer(f"🗑️ Deleted {title}", alert=True)
            text, buttons = await build_sync_rules_list_view(uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

        if action == "catchup" and arg.isdigit():
            rule = sync_manager.get_rule(int(arg))
            if not rule or not sync_manager.can_user_access_rule(rule, uid or 0, is_owner=owner_mode):
                await event.answer("Rule not found or unauthorized.", alert=True)
                return
            await event.answer("⚡ Starting catchup for last 5 posts...", alert=False)
            try:
                entity = await user_client.get_entity(PeerChannel(rule.tg_channel_id))
            except Exception as e:
                await event.respond(f"❌ Could not access channel entity: {e}")
                return

            status_msg = await event.respond(f"🔄 Fetching last 5 posts from **{rule.tg_title}**...")
            messages = await user_client.get_messages(entity, limit=5)
            if not messages:
                await status_msg.edit(f"ℹ️ No messages found in **{rule.tg_title}**.")
                return

            messages_chrono = list(reversed(messages))
            synced_count = 0
            for idx, m in enumerate(messages_chrono, start=1):
                try:
                    await status_msg.edit(
                        f"🔄 **[{idx}/{len(messages_chrono)}]** Syncing post `#{m.id}` to {rule.dest_icon} **{rule.dest_title}**..."
                    )
                except Exception:
                    pass
                ok = await sync_single_message_for_rule(user_client, bot_client, rule, m)
                if ok:
                    synced_count += 1
                await asyncio.sleep(1.0)

            await status_msg.edit(
                f"✅ **Catchup Complete!**\n\n"
                f"• Source: **{rule.tg_title}**\n"
                f"• {rule.dest_type_label}: **{rule.dest_title}**\n"
                f"• Synced: **{synced_count} of {len(messages_chrono)}** posts.",
                link_preview=False,
            )
            text, buttons = await build_sync_rule_detail_view(rule.id, uid)
            try:
                await event.edit(text, buttons=buttons, link_preview=False)
            except MessageNotModifiedError:
                pass
            return

    async def handle_add_tg_sync_rule(event: events.NewMessage.Event, subargs: str) -> None:
        """Create a Telegram-to-Telegram channel auto-sync rule (Free: max 1 rule; Paid/Owner: multiple)."""
        uid = event.sender_id or 0
        if not subargs:
            text, buttons = build_sync_add_tg_help_view(uid)
            await event.reply(text, buttons=buttons, link_preview=False)
            return

        # Pre-check Free user limit before resolving if they already have >= FREE_MAX_TG_SYNC_RULES
        if not is_paid_user(uid):
            current_tg_rules = sync_manager.count_user_tg_rules(uid)
            if current_tg_rules >= FREE_MAX_TG_SYNC_RULES:
                text, buttons = build_paid_upgrade_prompt(
                    uid,
                    reason=(
                        f"🔒 **Free Plan Limit Reached (`{current_tg_rules}/{FREE_MAX_TG_SYNC_RULES}` Telegram Auto-Sync Rule Used)**\n\n"
                        f"Free users can only have **{FREE_MAX_TG_SYNC_RULES} active Telegram Auto-Sync rule**.\n"
                        f"💡 _To sync a different channel for free, delete your existing rule in `/synclist` first — "
                        f"or upgrade to the **💎 Paid VIP Plan** for multiple Telegram syncs + WhatsApp sync!_"
                    ),
                )
                await event.reply(text, buttons=buttons, link_preview=False)
                return

        add_parts = subargs.split(None, 1)
        src_target = add_parts[0]
        dest_target = add_parts[1].strip().strip("\"'") if len(add_parts) > 1 else ""
        if dest_target.lower().startswith("tg:"):
            dest_target = dest_target[3:].strip()

        status_msg = await event.reply(f"⏳ Resolving source Telegram channel `{src_target}`...")
        src_entity, err = await resolve_channel_for_sync(user_client, src_target, bot_client=bot_client)
        if not src_entity:
            await status_msg.edit(f"❌ {err}")
            return

        ch_title = getattr(src_entity, "title", None) or getattr(src_entity, "username", None) or str(src_entity.id)
        ch_peer_id = utils.get_peer_id(src_entity)
        ch_num_id = src_entity.id
        ch_username = getattr(src_entity, "username", None)

        user_def = sync_manager.get_user_tg_channel(uid, TG_UPLOAD_CHANNEL_ID if is_owner(uid) else "")
        target_tg = dest_target or user_def
        if not target_tg:
            await status_msg.edit(
                f"⚠️ Source channel **{ch_title}** resolved, but no destination Telegram channel was specified and you haven't set a default yet.\n\n"
                f"**Run:**\n`/tgsync add {src_target} @your_destination_channel`\n\n"
                f"Or save your default destination channel first with:\n`/tgchannel @your_destination_channel`"
            )
            return

        await status_msg.edit(f"⏳ Resolving destination Telegram channel `{target_tg}`...")
        dest_entity, dest_err = await resolve_channel_for_sync(user_client, target_tg, bot_client=bot_client)
        if not dest_entity:
            await status_msg.edit(f"❌ Could not resolve destination channel `{target_tg}`: {dest_err}")
            return

        dest_num_id = dest_entity.id
        dest_peer_id = utils.get_peer_id(dest_entity)
        dest_title = getattr(dest_entity, "title", None) or getattr(dest_entity, "username", None) or str(dest_num_id)
        dest_username = getattr(dest_entity, "username", None)

        if normalize_channel_id(ch_num_id) == normalize_channel_id(dest_num_id):
            await status_msg.edit("❌ Source and destination Telegram channels cannot be the same.")
            return

        rule = sync_manager.add_tg_rule(
            tg_channel_id=ch_num_id,
            tg_peer_id=ch_peer_id,
            tg_title=ch_title,
            tg_username=ch_username,
            dest_tg_channel_id=dest_num_id,
            dest_tg_peer_id=dest_peer_id,
            dest_tg_title=dest_title,
            dest_tg_username=dest_username,
            send_text=TG_SYNC_SEND_TEXT,
            send_captions=TG_SYNC_SEND_CAPTIONS,
            owner_id=uid,
        )

        try:
            latest_msgs = await user_client.get_messages(src_entity, limit=1)
            if latest_msgs and rule.last_synced_msg_id is None:
                rule.last_synced_msg_id = latest_msgs[0].id
                sync_manager.save()
        except Exception:
            pass

        success_buttons = [
            [
                Button.inline("⚙️ Manage This Rule", f"sync:rule:{rule.id}".encode()),
                Button.inline("⚡ Catchup (5 Posts)", f"sync:catchup:{rule.id}".encode()),
            ],
            [
                Button.inline("📋 View My Rules", b"sync:list"),
                Button.inline("🏠 Main Menu", b"menu:home"),
            ],
        ]

        plan_note = (
            f"\n🆓 _Free Plan: `{sync_manager.count_user_tg_rules(uid)}/{FREE_MAX_TG_SYNC_RULES}` Telegram Sync rule active._"
            if not is_paid_user(uid)
            else ""
        )

        await status_msg.edit(
            f"✅ **Telegram Channel Auto-Sync Configured!**\n\n"
            f"📢 **Source TG:** **{rule.tg_title}** (`{rule.tg_peer_id}`)\n"
            f"✈️ **Destination TG:** **{rule.dest_title}** (`{rule.dest_id_display}`)\n"
            f"🆔 **Rule ID:** `#{rule.id}`\n"
            f"⚙️ **Settings:** 💬 Text: `{'ON' if rule.send_text else 'OFF'}` | 🏷️ Captions: `{'ON' if rule.send_captions else 'OFF'}`\n\n"
            f"🚀 New messages posted in **{rule.tg_title}** will now be automatically synced to **{rule.dest_title}**!"
            f"{plan_note}",
            buttons=success_buttons,
            link_preview=False,
        )

    @bot_client.on(
        events.NewMessage(pattern=r"^/(?:sync|autosync|tgsync)(?:@\w+)?(?:\s+([\s\S]+))?$")
    )
    async def sync_command_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return

        uid = event.sender_id or 0
        owner_mode = is_owner(uid)
        cmd_token = (event.raw_text or "").split(None, 1)[0].lower()
        is_tgsync_cmd = cmd_token.startswith("/tgsync")

        raw_args = (event.pattern_match.group(1) or "").strip()
        parts = raw_args.split(None, 1)
        subcmd = parts[0].lower() if parts else ""
        subargs = parts[1].strip() if len(parts) > 1 else ""

        # 1. Main Dashboard (with buttons)
        if not subcmd or subcmd in ("help", "?"):
            text, buttons = await build_sync_dashboard_view(uid)
            await event.reply(text, buttons=buttons, link_preview=False)
            return

        # 2. List Rules (with buttons)
        if subcmd in ("list", "ls"):
            text, buttons = await build_sync_rules_list_view(uid)
            await event.reply(text, buttons=buttons, link_preview=False)
            return

        # 3a. Add Telegram-to-Telegram Sync Rule (/tgsync add ... or /sync addtg ... or /sync tg ...)
        if subcmd in ("addtg", "tg") or (is_tgsync_cmd and subcmd == "add"):
            await handle_add_tg_sync_rule(event, subargs)
            return

        # 3b. Add Rule (/sync add ...)
        if subcmd == "add":
            if not subargs:
                text, buttons = build_sync_add_help_view(uid)
                await event.reply(text, buttons=buttons, link_preview=False)
                return

            add_parts = subargs.split(None, 1)
            tg_target = add_parts[0]
            wa_target = add_parts[1].strip() if len(add_parts) > 1 else ""

            # If destination explicitly looks like a Telegram channel (@..., -100..., t.me/..., tg:...), route to TG sync
            if wa_target and (
                wa_target.lower().startswith(("tg:", "@", "-100", "https://t.me/", "http://t.me/", "t.me/"))
            ):
                await handle_add_tg_sync_rule(event, subargs)
                return

            # Only Paid VIP Users & Owners can use WhatsApp Auto-Sync!
            if not is_paid_user(uid):
                text, buttons = build_paid_upgrade_prompt(
                    uid,
                    reason=(
                        "🔒 **WhatsApp Auto-Sync (`/sync add`) is available exclusively for 💎 Paid VIP Users!**\n\n"
                        "💡 _Want to sync to a Telegram channel instead? Free users get **1 Free Telegram Auto-Sync**! Use:_\n"
                        f"`/tgsync add {tg_target} @your_dest_channel`"
                    ),
                )
                await event.reply(text, buttons=buttons, link_preview=False)
                return

            status_msg = await event.reply(f"⏳ Resolving Telegram channel `{tg_target}`...")
            entity, err = await resolve_channel_for_sync(user_client, tg_target, bot_client=bot_client)
            if not entity:
                await status_msg.edit(f"❌ {err}")
                return

            ch_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
            ch_peer_id = utils.get_peer_id(entity)
            ch_num_id = entity.id
            ch_username = getattr(entity, "username", None)

            # Resolve WhatsApp destination
            target_wa = wa_target or WA_GROUP_ID
            if not target_wa:
                user_def = sync_manager.get_user_tg_channel(uid, TG_UPLOAD_CHANNEL_ID if owner_mode else "")
                if user_def:
                    await status_msg.delete()
                    await handle_add_tg_sync_rule(event, subargs)
                    return
                await status_msg.edit(
                    f"⚠️ Channel **{ch_title}** resolved, but no destination was specified and no default group/channel is set.\n\n"
                    f"• **For Telegram channel sync:** `/tgsync add {tg_target} <dest_tg_channel>`\n"
                    f"• **For WhatsApp group sync:** `/sync add {tg_target} <wa_group>`"
                )
                return

            try:
                wa_group = await wa_client.resolve_group(target_wa)
            except WhatsAppError as e:
                text, connected, _ = await wa_status_text()
                await status_msg.edit(
                    f"❌ Could not resolve WhatsApp group `{target_wa}`: {e}"
                    + ("" if connected else f"\n\n{text}\nUse `/walogin` to link WhatsApp first.")
                )
                return

            rule = sync_manager.add_rule(
                tg_channel_id=ch_num_id,
                tg_peer_id=ch_peer_id,
                tg_title=ch_title,
                tg_username=ch_username,
                wa_group_id=wa_group.id,
                wa_group_title=wa_group.subject,
                send_text=WA_SYNC_SEND_TEXT,
                send_captions=WA_SYNC_SEND_CAPTIONS,
                target_type="whatsapp",
                owner_id=uid,
            )

            # Initialize baseline to current latest post so poller doesn't backfill old history
            try:
                latest_msgs = await user_client.get_messages(entity, limit=1)
                if latest_msgs and rule.last_synced_msg_id is None:
                    rule.last_synced_msg_id = latest_msgs[0].id
                    sync_manager.save()
            except Exception:
                pass

            success_buttons = [
                [
                    Button.inline("⚙️ Manage This Rule", f"sync:rule:{rule.id}".encode()),
                    Button.inline("⚡ Catchup (5 Posts)", f"sync:catchup:{rule.id}".encode()),
                ],
                [
                    Button.inline("📋 View My Rules", b"sync:list"),
                    Button.inline("🏠 Main Menu", b"menu:home"),
                ],
            ]

            await status_msg.edit(
                f"✅ **WhatsApp Auto-Sync Configured!**\n\n"
                f"📢 **Telegram:** **{rule.tg_title}** (`{rule.tg_peer_id}`)\n"
                f"👥 **WhatsApp:** **{rule.wa_group_title}** (`{rule.wa_group_id}`)\n"
                f"🆔 **Rule ID:** `#{rule.id}`\n"
                f"⚙️ **Settings:** 💬 Text: `{'ON' if rule.send_text else 'OFF'}` | 🏷️ Captions: `{'ON' if rule.send_captions else 'OFF'}`\n\n"
                f"🚀 New messages posted in this channel will now be automatically sent to **{rule.wa_group_title}**!",
                buttons=success_buttons,
                link_preview=False,
            )
            return

        # 4. Remove / Delete Rule
        if subcmd in ("remove", "delete", "del", "rm"):
            if not subargs:
                await event.reply("⚠️ **Usage:** `/sync remove <rule_id | channel>`")
                return

            rule = sync_manager.find_rule(subargs, user_id=uid, is_owner=owner_mode)
            if not rule:
                await event.reply(f"❌ Could not find your sync rule matching `{subargs}`. Use `/synclist` to see IDs.")
                return

            removed = sync_manager.remove_rule(rule.id)
            if removed:
                await event.reply(
                    f"🗑️ **Auto-Sync Removed:** Rule `#{rule.id}` for **{rule.tg_title}** ➔ {rule.dest_icon} **{rule.dest_title}** deleted."
                )
            else:
                await event.reply(f"❌ Failed to remove rule `#{rule.id}`.")
            return

        # 5. Pause Rule
        if subcmd == "pause":
            if not subargs:
                await event.reply("⚠️ **Usage:** `/sync pause <rule_id | channel>`")
                return

            rule = sync_manager.find_rule(subargs, user_id=uid, is_owner=owner_mode)
            if not rule:
                await event.reply(f"❌ Could not find your sync rule matching `{subargs}`.")
                return

            sync_manager.toggle_rule(rule.id, enabled=False)
            await event.reply(
                f"⏸️ **Auto-Sync Paused:** Rule `#{rule.id}` (**{rule.tg_title}** ➔ {rule.dest_icon} **{rule.dest_title}**). Use `/sync resume {rule.id}` to re-enable."
            )
            return

        # 6. Resume Rule
        if subcmd in ("resume", "unpause", "start"):
            if not subargs:
                await event.reply("⚠️ **Usage:** `/sync resume <rule_id | channel>`")
                return

            rule = sync_manager.find_rule(subargs, user_id=uid, is_owner=owner_mode)
            if not rule:
                await event.reply(f"❌ Could not find your sync rule matching `{subargs}`.")
                return

            sync_manager.toggle_rule(rule.id, enabled=True)
            await event.reply(
                f"▶️ **Auto-Sync Resumed:** Rule `#{rule.id}` (**{rule.tg_title}** ➔ {rule.dest_icon} **{rule.dest_title}**) is now active."
            )
            return

        # 7. Text Toggle
        if subcmd == "text":
            toggle_parts = subargs.split()
            if len(toggle_parts) < 2 or toggle_parts[1].lower() not in ("on", "off", "1", "0", "true", "false"):
                await event.reply("⚠️ **Usage:** `/sync text <rule_id> <on|off>`")
                return

            rule = sync_manager.find_rule(toggle_parts[0], user_id=uid, is_owner=owner_mode)
            if not rule:
                await event.reply(f"❌ Could not find your rule `{toggle_parts[0]}`.")
                return

            new_val = toggle_parts[1].lower() in ("on", "1", "true")
            sync_manager.update_rule(rule.id, send_text=new_val)
            await event.reply(
                f"💬 Text-only sync set to **{'ON' if new_val else 'OFF'}** for `#{rule.id}` (**{rule.tg_title}**)."
            )
            return

        # 8. Caption Toggle
        if subcmd in ("caption", "captions"):
            toggle_parts = subargs.split()
            if len(toggle_parts) < 2 or toggle_parts[1].lower() not in ("on", "off", "1", "0", "true", "false"):
                await event.reply("⚠️ **Usage:** `/sync caption <rule_id> <on|off>`")
                return

            rule = sync_manager.find_rule(toggle_parts[0], user_id=uid, is_owner=owner_mode)
            if not rule:
                await event.reply(f"❌ Could not find your rule `{toggle_parts[0]}`.")
                return

            new_val = toggle_parts[1].lower() in ("on", "1", "true")
            sync_manager.update_rule(rule.id, send_captions=new_val)
            await event.reply(
                f"🏷️ Caption forwarding set to **{'ON' if new_val else 'OFF'}** for `#{rule.id}` (**{rule.tg_title}**)."
            )
            return

        # 9. Notify Toggle (Owner only)
        if subcmd in ("notify", "notifications"):
            if not owner_mode:
                await event.reply("⛔ **Only the bot owner can toggle global sync notifications.**")
                return
            val = subargs.lower()
            if val not in ("on", "off", "1", "0", "true", "false"):
                cur = "ON 🔔" if sync_manager.notify_on_sync else "OFF 🔕"
                await event.reply(f"ℹ️ Admin sync notifications are currently **{cur}**.\nToggle with `/sync notify <on|off>`.")
                return

            new_val = val in ("on", "1", "true")
            sync_manager.notify_on_sync = new_val
            if new_val:
                sync_manager.notify_chat_id = event.chat_id
            sync_manager.save()
            await event.reply(
                f"{'🔔' if new_val else '🔕'} Telegram sync notifications turned **{'ON' if new_val else 'OFF'}**."
                + (f" Updates will be sent to chat `{event.chat_id}`." if new_val else "")
            )
            return

        # 10. Catchup (Backfill recent messages)
        if subcmd in ("catchup", "backfill", "syncnow"):
            catch_parts = subargs.split()
            if not catch_parts:
                await event.reply("⚠️ **Usage:** `/sync catchup <rule_id | channel> [count=5]`")
                return

            rule = sync_manager.find_rule(catch_parts[0], user_id=uid, is_owner=owner_mode)
            if not rule:
                await event.reply(f"❌ Could not find your rule `{catch_parts[0]}`.")
                return

            count = 5
            if len(catch_parts) > 1 and catch_parts[1].isdigit():
                count = min(max(1, int(catch_parts[1])), 30)

            status_msg = await event.reply(f"🔄 Fetching last **{count}** posts from **{rule.tg_title}**...")

            try:
                entity = await user_client.get_entity(PeerChannel(rule.tg_channel_id))
            except Exception as e:
                await status_msg.edit(f"❌ Could not access channel entity: {e}")
                return

            messages = await user_client.get_messages(entity, limit=count)
            if not messages:
                await status_msg.edit(f"ℹ️ No messages found in channel **{rule.tg_title}**.")
                return

            messages_chrono = list(reversed(messages))
            await status_msg.edit(
                f"🔄 Syncing **{len(messages_chrono)}** posts from **{rule.tg_title}** ➔ {rule.dest_icon} **{rule.dest_title}**..."
            )

            synced_count = 0
            for idx, m in enumerate(messages_chrono, start=1):
                try:
                    await status_msg.edit(
                        f"🔄 **[{idx}/{len(messages_chrono)}]** Syncing post `#{m.id}` to {rule.dest_icon} **{rule.dest_title}**..."
                    )
                except Exception:
                    pass

                ok = await sync_single_message_for_rule(user_client, bot_client, rule, m)
                if ok:
                    synced_count += 1
                await asyncio.sleep(1.0)

            await status_msg.edit(
                f"✅ **Catchup Complete!**\n\n"
                f"• Source: **{rule.tg_title}**\n"
                f"• {rule.dest_type_label}: **{rule.dest_title}**\n"
                f"• Synced: **{synced_count} of {len(messages_chrono)}** posts successfully.",
                link_preview=False,
            )
            return

        # 11. Status (with buttons)
        if subcmd == "status":
            text, buttons = await build_sync_status_view(uid)
            await event.reply(text, buttons=buttons, link_preview=False)
            return

        await event.reply(
            f"❓ Unknown auto-sync command: `{subcmd}`.\nUse `/sync` or `/tgsync` to open the interactive Auto-Sync Dashboard."
        )

    @bot_client.on(events.NewMessage(pattern=r"^/(?:synclist|tgsynclist)(?:@\w+)?$"))
    async def synclist_alias_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return
        text, buttons = await build_sync_rules_list_view(event.sender_id)
        await event.reply(text, buttons=buttons, link_preview=False)

    @bot_client.on(events.NewMessage(pattern=r"^/(?:unsync|tgunsync)(?:@\w+)?(?:\s+(.+))?$"))
    async def unsync_handler(event: events.NewMessage.Event) -> None:
        if not is_authorized(event.sender_id):
            await event.reply("⛔ **Unauthorized.**")
            return
        uid = event.sender_id or 0
        target = (event.pattern_match.group(1) or "").strip()
        if not target:
            await event.reply("Usage: `/unsync <rule_id | channel>` (see `/synclist`)")
            return
        rule = sync_manager.find_rule(target, user_id=uid, is_owner=is_owner(uid))
        if not rule:
            await event.reply(f"❌ Could not find your sync rule matching `{target}`.")
            return
        sync_manager.remove_rule(rule.id)
        await event.reply(
            f"🗑️ **Auto-Sync Removed:** Rule `#{rule.id}` for **{rule.tg_title}** ➔ {rule.dest_icon} **{rule.dest_title}** deleted."
        )

    @bot_client.on(events.NewMessage)
    async def message_handler(event: events.NewMessage.Event) -> None:
        raw_text = (event.raw_text or "").strip()
        if not raw_text:
            return

        # Skip commands handled by dedicated handlers above
        if raw_text.startswith(
            (
                "/start",
                "/help",
                "/menu",
                "/myplan",
                "/plan",
                "/upgrade",
                "/vip",
                "/status",
                "/leave",
                "/url",
                "/newtunnel",
                "/cancel",
                "/stop",
                "/addpaid",
                "/adduser",
                "/auth",
                "/allow",
                "/removepaid",
                "/removeuser",
                "/deluser",
                "/rmuser",
                "/unauth",
                "/paidusers",
                "/users",
                "/listusers",
                "/admins",
                "/wa",
                "/whatsapp",
                "/tg",
                "/tgupload",
                "/upload",
                "/sync",
                "/autosync",
                "/tgsync",
                "/synclist",
                "/tgsynclist",
                "/unsync",
                "/tgunsync",
            )
        ):
            return

        if not is_authorized(event.sender_id):
            await event.reply(
                f"⛔ **Unauthorized.** Your Telegram ID (`{event.sender_id}`) is not allowed."
            )
            return

        is_join_cmd = raw_text.startswith("/join")
        is_dl_cmd = raw_text.startswith(("/dl", "/download", "/bulk", "/batch"))
        is_stream_cmd = raw_text.startswith("/stream")

        # 1. Check for message/file post links (unless explicitly running /join)
        if not is_join_cmd:
            post_links = extract_post_links(raw_text)
            if post_links:
                multi_links = len(post_links) > 1
                if multi_links:
                    ACTIVE_BULK_CHATS.add(event.chat_id)
                    CANCELLED_CHATS.discard(event.chat_id)
                try:
                    for link_idx, post in enumerate(post_links, start=1):
                        if event.chat_id in CANCELLED_CHATS:
                            await bot_client.send_message(
                                event.chat_id,
                                "🛑 **Bulk download cancelled.**",
                                reply_to=event.id,
                            )
                            break
                        await fetch_and_send_post(
                            user_client,
                            bot_client,
                            event,
                            post,
                            force_stream_only=is_stream_cmd,
                            force_telegram_upload=is_dl_cmd,
                        )
                        if multi_links and link_idx < len(post_links):
                            if (
                                BULK_COOLDOWN_EVERY > 0
                                and BULK_COOLDOWN_SECONDS > 0
                                and link_idx % BULK_COOLDOWN_EVERY == 0
                            ):
                                await _cancellable_sleep(event.chat_id, BULK_COOLDOWN_SECONDS)
                            elif BULK_DELAY_SECONDS > 0:
                                await _cancellable_sleep(event.chat_id, BULK_DELAY_SECONDS)
                finally:
                    if multi_links:
                        ACTIVE_BULK_CHATS.discard(event.chat_id)
                        CANCELLED_CHATS.discard(event.chat_id)
                return
            elif is_dl_cmd or is_stream_cmd:
                await event.reply(
                    "⚠️ **Please provide valid Telegram post link(s):**\n"
                    "• **Single post:** `https://t.me/c/4351613423/2`\n"
                    "• **Batch range:** `https://t.me/c/4351613423/2-50`\n"
                    "• **Start to end:** `/bulk https://t.me/c/4351613423/2 https://t.me/c/4351613423/50`"
                )
                return

        # 2. Otherwise, check for channel join links / @usernames / channel IDs
        targets = extract_join_targets(raw_text)
        if not targets:
            if is_join_cmd:
                await event.reply(
                    "⚠️ **Please provide a valid channel link, `@username`, or channel ID:**\n"
                    "Example: `/join https://t.me/telegram https://t.me/+InviteHash -1001234567890`"
                )
            elif event.is_private:
                await event.reply(
                    "👋 **Not sure what to send? Here is how to use me:**\n\n"
                    "• 🔗 **To Join a Channel:** Send `@channel`, channel ID (`-100...`), or a private invite link (`https://t.me/+...`)\n"
                    "• 📥 **To Download Posts:** Paste any post link (`https://t.me/c/.../12`) or range (`.../12-30`)\n"
                    "• ✈️ **To Auto-Sync a Channel:** Tap **🔄 Auto-Sync Dashboard** below (`1 Free TG Sync` | `💎 Paid for WhatsApp`)",
                    buttons=[
                        [
                            Button.inline("🏠 Open Main Menu", b"menu:home"),
                            Button.inline("🔄 Auto-Sync Dashboard", b"sync:main"),
                        ],
                        [
                            Button.inline("📥 Download Guide", b"menu:dl_guide"),
                            Button.inline("💎 My Plan / Upgrade", b"menu:plan"),
                        ],
                    ],
                    link_preview=False,
                )
            return

        progress_msg = await event.reply(
            f"🔄 Joining **{len(targets)}** channel(s)..."
        )

        results: List[str] = []
        for idx, target in enumerate(targets, start=1):
            _, msg = await join_single_target(user_client, target, bot_client=bot_client)
            results.append(f"{idx}. {msg}")

            if len(targets) > 1:
                summary = "\n".join(results)
                if idx < len(targets):
                    summary += f"\n\n⏳ Joining remaining ({idx}/{len(targets)})..."
                await progress_msg.edit(summary, link_preview=False)
                if idx < len(targets):
                    await asyncio.sleep(JOIN_DELAY_SECONDS)

        final_report = "\n".join(results)
        await progress_msg.edit(final_report, link_preview=False)

    try:
        await asyncio.gather(
            user_client.run_until_disconnected(),
            bot_client.run_until_disconnected(),
            payment_task,
        )
    finally:
        forward_task.cancel()
        sync_task.cancel()
        sync_poller_task.cancel()
        payment_task.cancel()
        await stop_stream_server()
        await wa_client.stop_sidecar()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down bot...")
