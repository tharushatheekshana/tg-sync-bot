"""
Telegram Auto-Sync Manager (WhatsApp Groups & Telegram Channels).

Manages persistent configuration and tracking for auto-syncing Telegram
channels to WhatsApp groups and/or destination Telegram channels.
"""
import json
import logging
import os
import tempfile
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger("sync-manager")


def normalize_channel_id(val: Union[int, str]) -> int:
    """Normalize any Telegram peer/chat/channel ID to positive channel ID integer."""
    try:
        s = str(abs(int(val)))
        if s.startswith("100") and len(s) > 10:
            return int(s[3:])
        return int(s)
    except Exception:
        return 0


def extract_media_unique_id(msg) -> Optional[str]:
    """
    Extract a stable unique file identifier from a Telethon Message object.
    Works across reposts, forwards, and different channels for the exact same Telegram media.
    """
    if not msg:
        return None
    media = getattr(msg, "media", None)
    if not media:
        return None
    doc = getattr(msg, "document", None) or getattr(media, "document", None)
    if doc and getattr(doc, "id", None):
        return f"doc:{doc.id}"
    photo = getattr(msg, "photo", None) or getattr(media, "photo", None)
    if photo and getattr(photo, "id", None):
        return f"photo:{photo.id}"
    file_obj = getattr(msg, "file", None)
    if file_obj and getattr(file_obj, "id", None):
        return f"file:{file_obj.id}"
    return None


@dataclass
class SyncRule:
    id: int
    tg_channel_id: int  # Source channel ID e.g. 1234567890
    tg_peer_id: int  # Source peer ID e.g. -1001234567890
    tg_title: str  # Source channel title
    tg_username: Optional[str]  # Source channel @username if public
    wa_group_id: str = ""  # WhatsApp group JID (e.g. ...@g.us)
    wa_group_title: str = ""  # WhatsApp group subject
    enabled: bool = True
    send_text: bool = True  # Sync text-only messages
    send_captions: bool = True  # Forward media captions
    created_at: str = ""
    last_synced_at: Optional[str] = None
    last_synced_msg_id: Optional[int] = None
    synced_count: int = 0
    target_type: str = "whatsapp"  # "whatsapp" | "telegram"
    dest_tg_channel_id: int = 0  # Destination Telegram channel ID
    dest_tg_peer_id: int = 0  # Destination Telegram peer ID (-100...)
    dest_tg_title: str = ""  # Destination Telegram channel title
    dest_tg_username: Optional[str] = None  # Destination Telegram @username
    owner_id: int = 0  # Telegram user ID who created this rule

    @property
    def is_telegram_target(self) -> bool:
        return self.target_type == "telegram"

    @property
    def dest_title(self) -> str:
        if self.is_telegram_target:
            return self.dest_tg_title or "Unknown Telegram Channel"
        return self.wa_group_title or "Unknown Group"

    @property
    def dest_id_display(self) -> str:
        if self.is_telegram_target:
            if self.dest_tg_username:
                return f"@{self.dest_tg_username}"
            return str(self.dest_tg_peer_id or self.dest_tg_channel_id)
        return self.wa_group_id

    @property
    def dest_badge(self) -> str:
        return "✈️ Telegram" if self.is_telegram_target else "📲 WhatsApp"

    @property
    def dest_type_label(self) -> str:
        return "Telegram Channel" if self.is_telegram_target else "WhatsApp Group"

    @property
    def dest_icon(self) -> str:
        return "✈️" if self.is_telegram_target else "📲"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SyncRule":
        raw_target_type = str(data.get("target_type") or "").strip().lower()
        dest_tg_id = int(data.get("dest_tg_channel_id", 0) or 0)
        dest_tg_peer = int(data.get("dest_tg_peer_id", 0) or 0)
        wa_gid = str(data.get("wa_group_id") or "")
        if raw_target_type in ("telegram", "tg"):
            target_type = "telegram"
        elif raw_target_type in ("whatsapp", "wa"):
            target_type = "whatsapp"
        elif (dest_tg_id or dest_tg_peer) and not wa_gid:
            target_type = "telegram"
        else:
            target_type = "whatsapp"

        return cls(
            id=int(data.get("id", 0)),
            tg_channel_id=int(data.get("tg_channel_id", 0)),
            tg_peer_id=int(data.get("tg_peer_id", 0)),
            tg_title=str(data.get("tg_title") or "Unknown Channel"),
            tg_username=data.get("tg_username") or None,
            wa_group_id=wa_gid,
            wa_group_title=str(data.get("wa_group_title") or ("Unknown Group" if target_type == "whatsapp" else "")),
            enabled=bool(data.get("enabled", True)),
            send_text=bool(data.get("send_text", True)),
            send_captions=bool(data.get("send_captions", True)),
            created_at=str(data.get("created_at") or ""),
            last_synced_at=data.get("last_synced_at") or None,
            last_synced_msg_id=data.get("last_synced_msg_id") or None,
            synced_count=int(data.get("synced_count", 0)),
            target_type=target_type,
            dest_tg_channel_id=dest_tg_id,
            dest_tg_peer_id=dest_tg_peer,
            dest_tg_title=str(data.get("dest_tg_title") or ""),
            dest_tg_username=data.get("dest_tg_username") or None,
            owner_id=int(data.get("owner_id", 0) or 0),
        )


class SyncManager:
    """Manages channel sync rules and state persistence."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.rules: Dict[int, SyncRule] = {}
        self.notify_on_sync: bool = False
        self.notify_chat_id: Optional[int] = None
        self.user_tg_channels: Dict[str, str] = {}
        self._next_id: int = 1
        self._recent_msgs: OrderedDict[Tuple[int, int], float] = OrderedDict()
        self._max_recent: int = 5000
        self.wa_uploaded_files: Dict[str, OrderedDict[str, None]] = {}
        self._max_wa_files_per_group: int = 5000
        self.load()

    def get_user_tg_channel(self, user_id: Optional[int], default_fallback: str = "") -> str:
        """Return the user's saved default Telegram destination channel, or fallback."""
        if user_id:
            val = self.user_tg_channels.get(str(user_id), "").strip()
            if val:
                return val
        return default_fallback

    def set_user_tg_channel(self, user_id: int, channel_str: str) -> None:
        """Save a user's default Telegram destination channel."""
        self.user_tg_channels[str(user_id)] = channel_str.strip()
        self.save()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Load sync rules from config file."""
        if not os.path.exists(self.config_path):
            logger.info("No sync config file found at %s. Starting fresh.", self.config_path)
            return

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            settings = data.get("settings", {})
            self.notify_on_sync = bool(settings.get("notify_on_sync", False))
            self.notify_chat_id = settings.get("notify_chat_id")
            raw_user_ch = settings.get("user_tg_channels") or {}
            if isinstance(raw_user_ch, dict):
                self.user_tg_channels = {str(k): str(v) for k, v in raw_user_ch.items() if v}

            self.wa_uploaded_files.clear()
            raw_wa_files = settings.get("wa_uploaded_files") or data.get("wa_uploaded_files") or {}
            if isinstance(raw_wa_files, dict):
                for jid, ids in raw_wa_files.items():
                    if isinstance(ids, list):
                        od: OrderedDict[str, None] = OrderedDict()
                        for uid in ids[-self._max_wa_files_per_group :]:
                            if uid:
                                od[str(uid)] = None
                        self.wa_uploaded_files[str(jid)] = od

            self.rules.clear()
            max_id = 0
            for r_data in data.get("rules", []):
                try:
                    rule = SyncRule.from_dict(r_data)
                    self.rules[rule.id] = rule
                    if rule.id > max_id:
                        max_id = rule.id
                except Exception as err:
                    logger.warning("Failed to parse sync rule: %s (%s)", r_data, err)

            self._next_id = max_id + 1
            logger.info("Loaded %d sync rules from %s", len(self.rules), self.config_path)
        except Exception as e:
            logger.exception("Failed to load sync configuration from %s: %s", self.config_path, e)

    def save(self) -> None:
        """Atomically persist sync rules to config file."""
        data = {
            "settings": {
                "notify_on_sync": self.notify_on_sync,
                "notify_chat_id": self.notify_chat_id,
                "user_tg_channels": self.user_tg_channels,
                "wa_uploaded_files": {
                    jid: list(od.keys()) for jid, od in self.wa_uploaded_files.items() if od
                },
            },
            "rules": [rule.to_dict() for rule in self.rules.values()],
        }

        config_dir = os.path.dirname(os.path.abspath(self.config_path))
        os.makedirs(config_dir, exist_ok=True)

        try:
            temp_file = tempfile.NamedTemporaryFile(
                "w", dir=config_dir, delete=False, encoding="utf-8"
            )
            with temp_file as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(temp_file.name, self.config_path)
        except Exception as e:
            logger.exception("Failed to save sync configuration to %s: %s", self.config_path, e)
            if os.path.exists(temp_file.name):
                try:
                    os.remove(temp_file.name)
                except OSError:
                    pass

    # ------------------------------------------------------------------
    # Query & Lookup
    # ------------------------------------------------------------------

    def get_all_rules(self) -> List[SyncRule]:
        """Return all sync rules sorted by ID."""
        return sorted(self.rules.values(), key=lambda r: r.id)

    def get_rules_for_user(self, user_id: int, is_owner: bool = False) -> List[SyncRule]:
        """Return rules visible to user_id (all rules for owners, or rules created by user_id)."""
        if is_owner:
            return self.get_all_rules()
        return sorted(
            (r for r in self.rules.values() if r.owner_id == user_id),
            key=lambda r: r.id,
        )

    def count_user_tg_rules(self, user_id: int) -> int:
        """Count how many Telegram-to-Telegram sync rules a specific user owns."""
        return sum(
            1
            for r in self.rules.values()
            if r.owner_id == user_id and r.is_telegram_target
        )

    def can_user_access_rule(
        self, rule: Optional[SyncRule], user_id: int, is_owner: bool = False
    ) -> bool:
        """Check whether user_id is allowed to view/modify a sync rule."""
        if not rule:
            return False
        if is_owner:
            return True
        return rule.owner_id == user_id

    def get_rule(self, rule_id: int) -> Optional[SyncRule]:
        """Get rule by numeric ID."""
        return self.rules.get(rule_id)

    def get_rules_for_channel(self, chat_id: int) -> List[SyncRule]:
        """
        Return all sync rules whose source Telegram channel matches chat_id.
        Matches by full peer ID (-100...) or numeric channel ID.
        """
        norm_chat = normalize_channel_id(chat_id)
        matched: List[SyncRule] = []
        for rule in self.rules.values():
            if norm_chat and norm_chat in (
                normalize_channel_id(rule.tg_channel_id),
                normalize_channel_id(rule.tg_peer_id),
            ):
                matched.append(rule)
            elif chat_id in (rule.tg_peer_id, rule.tg_channel_id):
                matched.append(rule)
        return sorted(matched, key=lambda r: r.id)

    def get_rule_for_channel(
        self, chat_id: int, target_type: Optional[str] = None
    ) -> Optional[SyncRule]:
        """
        Fast lookup to check if a Telegram chat/channel matches a rule.
        Matches by full peer ID (-100...) or numeric channel ID, optionally filtered by target_type.
        """
        for rule in self.get_rules_for_channel(chat_id):
            if target_type is None or rule.target_type == target_type:
                return rule
        return None

    def find_rule(
        self,
        query: Union[int, str],
        user_id: Optional[int] = None,
        is_owner: bool = True,
    ) -> Optional[SyncRule]:
        """
        Find a rule by ID, channel ID, username, or partial title.
        When user_id is provided and is_owner is False, only searches that user's rules.
        """
        candidates = (
            self.get_all_rules()
            if (is_owner or user_id is None)
            else self.get_rules_for_user(user_id, is_owner=False)
        )
        candidate_ids = {r.id for r in candidates}

        if isinstance(query, int):
            if query in candidate_ids:
                return self.rules[query]
            for r in candidates:
                if normalize_channel_id(query) in (
                    normalize_channel_id(r.tg_channel_id),
                    normalize_channel_id(r.tg_peer_id),
                ):
                    return r
            return None

        s = str(query).strip()
        if not s:
            return None

        # Clean leading # or @
        if s.startswith("#") and s[1:].isdigit():
            s = s[1:]
        if s.isdigit():
            val = int(s)
            if val in candidate_ids:
                return self.rules[val]
            norm_val = normalize_channel_id(val)
            for r in candidates:
                if norm_val and norm_val in (
                    normalize_channel_id(r.tg_channel_id),
                    normalize_channel_id(r.tg_peer_id),
                ):
                    return r

        clean_user = s.lstrip("@").lower()
        for rule in candidates:
            if rule.tg_username and rule.tg_username.lower() == clean_user:
                return rule
            if rule.dest_tg_username and rule.dest_tg_username.lower() == clean_user:
                return rule

        # Match by partial source or destination title
        low_query = s.lower()
        for rule in candidates:
            if (
                low_query in rule.tg_title.lower()
                or low_query in rule.wa_group_title.lower()
                or low_query in rule.dest_tg_title.lower()
            ):
                return rule

        return None

    # ------------------------------------------------------------------
    # Rule Mutations
    # ------------------------------------------------------------------

    def find_existing_rule(
        self,
        tg_channel_id: int,
        target_type: str = "whatsapp",
        wa_group_id: str = "",
        dest_tg_channel_id: int = 0,
    ) -> Optional[SyncRule]:
        """Check if an identical source -> destination sync rule already exists."""
        for r in self.get_rules_for_channel(tg_channel_id):
            if r.target_type != target_type:
                continue
            if target_type == "whatsapp" and (r.wa_group_id == wa_group_id or not r.wa_group_id):
                return r
            if target_type == "telegram" and (
                normalize_channel_id(r.dest_tg_channel_id) == normalize_channel_id(dest_tg_channel_id)
                or not r.dest_tg_channel_id
            ):
                return r
        return None

    def add_rule(
        self,
        tg_channel_id: int,
        tg_peer_id: int,
        tg_title: str,
        tg_username: Optional[str],
        wa_group_id: str = "",
        wa_group_title: str = "",
        send_text: bool = True,
        send_captions: bool = True,
        target_type: str = "whatsapp",
        dest_tg_channel_id: int = 0,
        dest_tg_peer_id: int = 0,
        dest_tg_title: str = "",
        dest_tg_username: Optional[str] = None,
        owner_id: int = 0,
    ) -> SyncRule:
        """Add a new WhatsApp or Telegram sync rule and save to file."""
        existing = self.find_existing_rule(
            tg_channel_id=tg_channel_id,
            target_type=target_type,
            wa_group_id=wa_group_id,
            dest_tg_channel_id=dest_tg_channel_id,
        )

        if existing:
            existing.tg_title = tg_title
            existing.tg_username = tg_username
            existing.wa_group_id = wa_group_id
            existing.wa_group_title = wa_group_title
            existing.target_type = target_type
            existing.dest_tg_channel_id = dest_tg_channel_id
            existing.dest_tg_peer_id = dest_tg_peer_id
            existing.dest_tg_title = dest_tg_title
            existing.dest_tg_username = dest_tg_username
            existing.send_text = send_text
            existing.send_captions = send_captions
            if owner_id and not existing.owner_id:
                existing.owner_id = owner_id
            existing.enabled = True
            self.save()
            return existing

        rule_id = self._next_id
        self._next_id += 1
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        rule = SyncRule(
            id=rule_id,
            tg_channel_id=tg_channel_id,
            tg_peer_id=tg_peer_id,
            tg_title=tg_title,
            tg_username=tg_username,
            wa_group_id=wa_group_id,
            wa_group_title=wa_group_title,
            enabled=True,
            send_text=send_text,
            send_captions=send_captions,
            created_at=now_str,
            synced_count=0,
            target_type=target_type,
            dest_tg_channel_id=dest_tg_channel_id,
            dest_tg_peer_id=dest_tg_peer_id,
            dest_tg_title=dest_tg_title,
            dest_tg_username=dest_tg_username,
            owner_id=owner_id,
        )
        self.rules[rule_id] = rule
        self.save()
        logger.info(
            "Added AutoSync rule #%d [%s] (owner=%s): '%s' (%s) -> '%s' (%s)",
            rule_id,
            target_type,
            owner_id,
            tg_title,
            tg_peer_id,
            rule.dest_title,
            rule.dest_id_display,
        )
        return rule

    def add_tg_rule(
        self,
        tg_channel_id: int,
        tg_peer_id: int,
        tg_title: str,
        tg_username: Optional[str],
        dest_tg_channel_id: int,
        dest_tg_peer_id: int,
        dest_tg_title: str,
        dest_tg_username: Optional[str] = None,
        send_text: bool = True,
        send_captions: bool = True,
        owner_id: int = 0,
    ) -> SyncRule:
        """Convenience wrapper to add a Telegram-to-Telegram channel sync rule."""
        return self.add_rule(
            tg_channel_id=tg_channel_id,
            tg_peer_id=tg_peer_id,
            tg_title=tg_title,
            tg_username=tg_username,
            send_text=send_text,
            send_captions=send_captions,
            target_type="telegram",
            dest_tg_channel_id=dest_tg_channel_id,
            dest_tg_peer_id=dest_tg_peer_id,
            dest_tg_title=dest_tg_title,
            dest_tg_username=dest_tg_username,
            owner_id=owner_id,
        )

    def remove_rule(self, rule_id: int) -> Optional[SyncRule]:
        """Remove a rule by ID and save."""
        rule = self.rules.pop(rule_id, None)
        if rule:
            self.save()
            logger.info("Removed AutoSync rule #%d", rule_id)
        return rule

    def toggle_rule(self, rule_id: int, enabled: Optional[bool] = None) -> Optional[SyncRule]:
        """Toggle or set enabled status for a rule."""
        rule = self.rules.get(rule_id)
        if not rule:
            return None
        if enabled is None:
            rule.enabled = not rule.enabled
        else:
            rule.enabled = bool(enabled)
        self.save()
        return rule

    def update_rule(self, rule_id: int, **kwargs) -> Optional[SyncRule]:
        """Update arbitrary attributes of a rule."""
        rule = self.rules.get(rule_id)
        if not rule:
            return None
        for k, v in kwargs.items():
            if hasattr(rule, k):
                setattr(rule, k, v)
        self.save()
        return rule

    def record_synced(self, rule_id: int, msg_id: int) -> None:
        """Update last synced message ID and count."""
        rule = self.rules.get(rule_id)
        if rule:
            rule.synced_count += 1
            if rule.last_synced_msg_id is None or msg_id > rule.last_synced_msg_id:
                rule.last_synced_msg_id = msg_id
            rule.last_synced_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            self.save()

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def is_recently_synced(self, channel_id: int, msg_id: int) -> bool:
        """Check if message was recently queued or synced."""
        return (channel_id, msg_id) in self._recent_msgs

    def mark_recently_synced(self, channel_id: int, msg_id: int) -> None:
        """Mark message as queued or synced."""
        key = (channel_id, msg_id)
        self._recent_msgs[key] = time.time()
        # Bound size
        while len(self._recent_msgs) > self._max_recent:
            self._recent_msgs.popitem(last=False)

    def is_wa_file_uploaded(self, wa_group_id: str, unique_id: Optional[str]) -> bool:
        """Check if a file with this unique_id was already uploaded to the given WhatsApp group."""
        if not wa_group_id or not unique_id:
            return False
        group_map = self.wa_uploaded_files.get(str(wa_group_id))
        return bool(group_map and str(unique_id) in group_map)

    def mark_wa_file_uploaded(
        self, wa_group_id: str, unique_id: Optional[str], persist: bool = True
    ) -> None:
        """Record that a file with this unique_id has been uploaded to the given WhatsApp group."""
        if not wa_group_id or not unique_id:
            return
        jid = str(wa_group_id)
        uid = str(unique_id)
        group_map = self.wa_uploaded_files.setdefault(jid, OrderedDict())
        if uid in group_map:
            group_map.move_to_end(uid)
        else:
            group_map[uid] = None
            while len(group_map) > self._max_wa_files_per_group:
                group_map.popitem(last=False)
        if persist:
            self.save()
