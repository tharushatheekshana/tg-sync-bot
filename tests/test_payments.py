import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from payments import SubscriptionStore, TelegramStarsPayments, add_one_month_timestamp


class PaymentStoreTests(unittest.TestCase):
    def test_add_one_month_clamps_end_of_month(self):
        jan_31 = int(datetime(2026, 1, 31, 9, tzinfo=timezone.utc).timestamp())
        expiry = datetime.fromtimestamp(add_one_month_timestamp(jan_31), timezone.utc)
        self.assertEqual((expiry.year, expiry.month, expiry.day, expiry.hour), (2026, 2, 28, 9))

    def test_successful_payment_activates_for_one_month_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SubscriptionStore(Path(directory) / "payments.sqlite3")
            expires = store.record_payment(
                user_id=123,
                charge_id="charge-1",
                paid_at=1793232000,
                amount=50,
                currency="XTR",
            )
            self.assertEqual(expires, store.get_expiry(123))
            self.assertTrue(store.is_active(123, now=1793232001))
            self.assertEqual(
                expires,
                store.record_payment(123, "charge-1", 1793232000, 50, "XTR"),
            )
            self.assertFalse(store.is_active(123, now=expires))

    def test_renewal_extends_from_current_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SubscriptionStore(Path(directory) / "payments.sqlite3")
            first_paid = int(datetime(2026, 1, 31, 9, tzinfo=timezone.utc).timestamp())
            second_paid = int(datetime(2026, 2, 10, 9, tzinfo=timezone.utc).timestamp())
            store.record_payment(123, "charge-1", first_paid, 50, "XTR")
            expiry = store.record_payment(123, "charge-2", second_paid, 50, "XTR")
            self.assertEqual(
                datetime.fromtimestamp(expiry, timezone.utc),
                datetime(2026, 3, 28, 9, tzinfo=timezone.utc),
            )

    def test_refund_recalculates_remaining_access(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SubscriptionStore(Path(directory) / "payments.sqlite3")
            first_paid = int(datetime(2026, 1, 31, 9, tzinfo=timezone.utc).timestamp())
            second_paid = int(datetime(2026, 2, 10, 9, tzinfo=timezone.utc).timestamp())
            store.record_payment(123, "charge-1", first_paid, 50, "XTR")
            store.record_payment(123, "charge-2", second_paid, 50, "XTR")
            expiry = store.mark_refunded(123, "charge-1")
            self.assertEqual(
                datetime.fromtimestamp(expiry, timezone.utc),
                datetime(2026, 3, 10, 9, tzinfo=timezone.utc),
            )
            self.assertTrue(store.get_payment_refunded(123, "charge-1"))
            self.assertEqual(store.get_latest_unrefunded_charge(123), "charge-2")

    def test_rejects_unexpected_amount_or_currency(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SubscriptionStore(Path(directory) / "payments.sqlite3")
            with self.assertRaises(ValueError):
                store.record_payment(123, "charge-2", 1793232000, 49, "XTR")
            with self.assertRaises(ValueError):
                store.record_payment(123, "charge-3", 1793232000, 50, "USD")


class StarsFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_invoice_uses_50_xtr_and_single_calendar_month(self):
        payments = TelegramStarsPayments("not-a-real-token", SubscriptionStore(":memory:"), lambda *_: None)
        captured = {}

        async def fake_api(method, payload):
            captured.update(method=method, payload=payload)
            return 1

        payments._api = fake_api
        await payments.send_invoice(456)
        self.assertEqual(captured["method"], "sendInvoice")
        self.assertEqual(captured["payload"]["currency"], "XTR")
        self.assertEqual(captured["payload"]["prices"][0]["amount"], 50)
        self.assertEqual(captured["payload"]["start_parameter"], "")
        self.assertTrue(captured["payload"]["payload"].startswith("vip-monthly:456:"))

    async def test_precheckout_checks_user_amount_currency_and_payload(self):
        payments = TelegramStarsPayments("not-a-real-token", SubscriptionStore(":memory:"), lambda *_: None)
        captured = []

        async def fake_api(method, payload):
            captured.append((method, payload))
            return True

        payments._api = fake_api
        await payments._handle_update({"pre_checkout_query": {
            "id": "query-1", "from": {"id": 789},
            "invoice_payload": "vip-monthly:789:nonce", "currency": "XTR", "total_amount": 50,
        }})
        self.assertEqual(captured[0][0], "answerPreCheckoutQuery")
        self.assertTrue(captured[0][1]["ok"])

    async def test_refund_uses_telegram_payment_charge_id(self):
        payments = TelegramStarsPayments("not-a-real-token", SubscriptionStore(":memory:"), lambda *_: None)
        captured = {}

        async def fake_api(method, payload):
            captured.update(method=method, payload=payload)
            return True

        payments._api = fake_api
        await payments.refund_stars(456, "charge-id")
        self.assertEqual(captured["method"], "refundStarPayment")
        self.assertEqual(captured["payload"]["user_id"], 456)
        self.assertEqual(captured["payload"]["telegram_payment_charge_id"], "charge-id")

    async def test_successful_payment_activates_subscription(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SubscriptionStore(Path(directory) / "payments.sqlite3")
            activated = []
            payments = TelegramStarsPayments("not-a-real-token", store, lambda uid, exp: _append(activated, uid, exp))
            calls = []

            async def fake_api(method, payload):
                calls.append((method, payload))
                return True

            payments._api = fake_api
            await payments._handle_update({"message": {
                "from": {"id": 987},
                "successful_payment": {
                    "invoice_payload": "vip-monthly:987:nonce",
                    "telegram_payment_charge_id": "charge-paid",
                    "currency": "XTR", "total_amount": 50,
                },
            }})
            self.assertTrue(store.is_active(987))
            self.assertEqual(activated[0][0], 987)
            self.assertEqual(calls[-1][0], "sendMessage")


async def _append(items, *values):
    items.append(values)


if __name__ == "__main__":
    unittest.main()
