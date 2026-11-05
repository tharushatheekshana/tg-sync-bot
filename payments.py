"""Telegram Stars checkout and durable monthly VIP subscription storage."""
from __future__ import annotations

import asyncio
import calendar
import logging
import secrets
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

import aiohttp

logger = logging.getLogger("channel-joiner.payments")
PRICE_STARS = 50
CURRENCY = "XTR"


def add_one_month_timestamp(timestamp: int) -> int:
    """Add one calendar month in UTC, clamping to the target month's last day."""
    dt = datetime.fromtimestamp(timestamp, timezone.utc)
    year, month = dt.year + (1 if dt.month == 12 else 0), (1 if dt.month == 12 else dt.month + 1)
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return int(dt.replace(year=year, month=month, day=day).timestamp())


class SubscriptionStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS subscriptions (user_id INTEGER PRIMARY KEY, expires_at INTEGER NOT NULL)")
            db.execute("CREATE TABLE IF NOT EXISTS payments (charge_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, paid_at INTEGER NOT NULL, amount INTEGER NOT NULL, currency TEXT NOT NULL, expires_at INTEGER NOT NULL, refunded INTEGER NOT NULL DEFAULT 0)")
            columns = {row[1] for row in db.execute("PRAGMA table_info(payments)")}
            if "refunded" not in columns:
                db.execute("ALTER TABLE payments ADD COLUMN refunded INTEGER NOT NULL DEFAULT 0")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=10)

    def get_expiry(self, user_id: int) -> int | None:
        with self._connect() as db:
            row = db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
        return int(row[0]) if row else None

    def is_active(self, user_id: int, now: int | None = None) -> bool:
        expiry = self.get_expiry(user_id)
        return expiry is not None and expiry > (int(time.time()) if now is None else now)

    def get_payment_refunded(self, user_id: int, charge_id: str) -> bool | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT refunded FROM payments WHERE charge_id=? AND user_id=?",
                (charge_id, user_id),
            ).fetchone()
        return bool(row[0]) if row else None

    def get_latest_unrefunded_charge(self, user_id: int) -> str | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT charge_id FROM payments WHERE user_id=? AND refunded=0 ORDER BY paid_at DESC, charge_id DESC LIMIT 1",
                (user_id,),
            ).fetchone()
        return str(row[0]) if row else None

    def mark_refunded(self, user_id: int, charge_id: str) -> int | None:
        """Mark a refunded charge and recalculate remaining paid access."""
        with self._connect() as db:
            row = db.execute(
                "SELECT refunded FROM payments WHERE charge_id=? AND user_id=?",
                (charge_id, user_id),
            ).fetchone()
            if row is None:
                raise ValueError("Payment record not found for that user")
            if row[0]:
                current = db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
                return int(current[0]) if current else None
            db.execute("UPDATE payments SET refunded=1 WHERE charge_id=? AND user_id=?", (charge_id, user_id))
            rows = db.execute(
                "SELECT paid_at FROM payments WHERE user_id=? AND refunded=0 ORDER BY paid_at, charge_id",
                (user_id,),
            ).fetchall()
            expiry: int | None = None
            for (paid_at,) in rows:
                expiry = add_one_month_timestamp(max(int(paid_at), expiry or int(paid_at)))
            if expiry is None:
                db.execute("DELETE FROM subscriptions WHERE user_id=?", (user_id,))
            else:
                db.execute(
                    "INSERT INTO subscriptions(user_id, expires_at) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at",
                    (user_id, expiry),
                )
            return expiry

    def record_payment(self, user_id: int, charge_id: str, paid_at: int, amount: int, currency: str) -> int:
        if amount != PRICE_STARS or currency != CURRENCY:
            raise ValueError("Unexpected payment amount or currency")
        with self._connect() as db:
            prior = db.execute("SELECT expires_at FROM payments WHERE charge_id=?", (charge_id,)).fetchone()
            if prior:
                if int(db.execute("SELECT user_id FROM payments WHERE charge_id=?", (charge_id,)).fetchone()[0]) != user_id:
                    raise ValueError("Charge ID is already associated with another user")
                return int(prior[0])
            current = db.execute("SELECT expires_at FROM subscriptions WHERE user_id=?", (user_id,)).fetchone()
            start = max(paid_at, int(current[0])) if current and int(current[0]) > paid_at else paid_at
            expires_at = add_one_month_timestamp(start)
            db.execute(
                "INSERT INTO payments(charge_id, user_id, paid_at, amount, currency, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
                (charge_id, user_id, paid_at, amount, currency, expires_at),
            )
            db.execute("INSERT INTO subscriptions(user_id, expires_at) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at", (user_id, expires_at))
            return expires_at


class TelegramStarsPayments:
    def __init__(self, bot_token: str, store: SubscriptionStore,
                 on_activated: Callable[[int, int], Awaitable[None]]):
        self.api = f"https://api.telegram.org/bot{bot_token}"
        self.store = store
        self.on_activated = on_activated
        self.session: aiohttp.ClientSession | None = None
        self.offset = 0

    async def _api(self, method: str, payload: dict | None = None) -> dict:
        assert self.session is not None
        async with self.session.post(f"{self.api}/{method}", json=payload or {}) as response:
            result = await response.json()
            if not result.get("ok"):
                raise RuntimeError(f"Telegram {method} failed: {result.get('description', 'unknown error')}")
            return result["result"]

    async def send_invoice(self, user_id: int) -> None:
        payload = f"vip-monthly:{user_id}:{secrets.token_urlsafe(12)}"
        await self._api("sendInvoice", {
            "chat_id": user_id,
            "title": "VIP Plan — 1 month",
            "description": "One calendar month of VIP features: WhatsApp sync/uploads and multiple Telegram sync rules.",
            "payload": payload,
            "currency": CURRENCY,
            "prices": [{"label": "VIP Plan — 1 month", "amount": PRICE_STARS}],
            "start_parameter": "",
        })

    async def refund_stars(self, user_id: int, charge_id: str) -> None:
        result = await self._api("refundStarPayment", {
            "user_id": user_id,
            "telegram_payment_charge_id": charge_id,
        })
        if not result:
            raise RuntimeError("Telegram did not confirm the Stars refund")

    async def _handle_update(self, update: dict) -> None:
        query = update.get("pre_checkout_query")
        if query:
            uid = int(query["from"]["id"])
            expected = f"vip-monthly:{uid}:"
            valid = (str(query.get("invoice_payload", "")).startswith(expected)
                     and query.get("currency") == CURRENCY
                     and query.get("total_amount") == PRICE_STARS)
            await self._api("answerPreCheckoutQuery", {
                "pre_checkout_query_id": query["id"],
                "ok": bool(valid),
                **({} if valid else {"error_message": "This invoice is invalid. Please start checkout again."}),
            })
            return

        message = update.get("message") or update.get("edited_message") or {}
        payment = message.get("successful_payment")
        if not payment:
            return
        uid = int(message.get("from", {}).get("id", 0))
        payload = str(payment.get("invoice_payload", ""))
        charge_id = str(payment.get("telegram_payment_charge_id", ""))
        if (not uid or not charge_id or not payload.startswith(f"vip-monthly:{uid}:")
                or payment.get("currency") != CURRENCY or payment.get("total_amount") != PRICE_STARS):
            logger.error("Ignoring invalid successful-payment update for user %s", uid)
            return
        expiry = self.store.record_payment(uid, charge_id, int(time.time()), PRICE_STARS, CURRENCY)
        await self.on_activated(uid, expiry)
        await self._api("sendMessage", {
            "chat_id": uid,
            "text": "✅ Payment received! Your VIP plan is active for one month. Use /myplan to check your status.",
        })

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=40)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            while True:
                try:
                    updates = await self._api("getUpdates", {
                        "offset": self.offset,
                        "limit": 10,
                        "timeout": 30,
                        "allowed_updates": ["message", "pre_checkout_query"],
                    })
                    for update in updates:
                        try:
                            await self._handle_update(update)
                        except Exception:
                            logger.exception("Failed to process Telegram payment update %s", update.get("update_id"))
                            break
                        self.offset = max(self.offset, int(update["update_id"]) + 1)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Telegram Stars polling failed; retrying shortly")
                    await asyncio.sleep(5)
            
