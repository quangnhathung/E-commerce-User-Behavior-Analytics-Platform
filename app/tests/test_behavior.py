"""Session and funnel business rules, independently of Spark/container dependencies."""
from datetime import datetime, timedelta, timezone
import hashlib
import random
import unittest

from helpers import APP
from ecommerce_behavior.contracts import (BEHAVIOR_VERSION, counts, iso, product_fingerprint,
                                         reference, session_id, validate_counts)


def event(index, milliseconds=0, visitor="001", item="10", kind="view"):
    return {"event_id": hashlib.sha256(str(index).encode()).hexdigest(), "visitor_id": visitor,
            "item_id": item, "event_type": kind,
            "event_time": iso(datetime(2015, 9, 10, 23, 45, tzinfo=timezone.utc) + timedelta(milliseconds=milliseconds))}


class BehaviorContractTests(unittest.TestCase):
    def test_thirty_minute_boundary_preserves_milliseconds_and_midnight(self):
        rows = [event(0), event(1, 1_800_000), event(2, 3_600_001)]
        result = reference(rows, "scope")
        self.assertEqual(2, result["summary"]["total_sessions"])
        first = next(row for row in result["sessions"] if row["event_count"] == 2)
        self.assertEqual("2015-09-10", first["session_start_date"])
        self.assertTrue(first["session_end"].startswith("2015-09-11"))

    def test_ordered_same_item_chain_counts_session_once(self):
        rows = [event(0), event(1, 1000, kind="add_to_cart"), event(2, 2000, kind="purchase"),
                event(3, 3000, kind="purchase"), event(4, 4000)]
        summary = reference(rows, "scope")["summary"]
        self.assertEqual((5, 1, 1, 1, 1), tuple(summary[key] for key in
            ("event_count", "total_sessions", "view_sessions", "cart_sessions", "purchase_sessions")))
        self.assertEqual(1, summary["view_to_purchase_rate"])

    def test_cross_item_cross_visitor_and_reverse_order_do_not_complete_chain(self):
        cases = ([event(0), event(1, 1, item="20", kind="add_to_cart"), event(2, 2, kind="purchase")],
                 [event(0), event(1, 1, visitor="2", kind="add_to_cart"), event(2, 2, kind="purchase")],
                 [event(0, kind="purchase"), event(1, 1, kind="add_to_cart"), event(2, 2)])
        for rows in cases:
            with self.subTest(rows=rows):
                self.assertEqual(0, reference(rows, "scope")["summary"]["purchase_sessions"])

    def test_equal_timestamp_is_not_a_business_order(self):
        for rows in ([event(0), event(1, kind="add_to_cart"), event(2, 1, kind="purchase")],
                     [event(0), event(1, 1, kind="add_to_cart"), event(2, 1, kind="purchase")]):
            with self.subTest(rows=rows):
                self.assertEqual(0, reference(rows, "scope")["summary"]["purchase_sessions"])

    def test_global_sessions_are_not_sum_of_product_sessions(self):
        rows = [event(0), event(1, 1, kind="add_to_cart"), event(2, 2, kind="purchase"),
                event(3, 3, item="20"), event(4, 4, item="20", kind="add_to_cart"),
                event(5, 5, item="20", kind="purchase")]
        result = reference(rows, "scope")
        self.assertEqual(1, result["summary"]["purchase_sessions"])
        self.assertEqual(2, sum(row["purchase_sessions"] for row in result["products"]))

    def test_missing_steps_preserve_session_and_event_counts(self):
        result = reference([event(0, kind="purchase")], "scope")
        self.assertEqual(1, result["summary"]["event_count"])
        self.assertEqual(1, result["summary"]["total_sessions"])
        self.assertEqual(0, result["summary"]["view_sessions"])
        self.assertEqual(0, result["summary"]["view_drop_rate"])
        self.assertEqual(1, result["sessions"][0]["purchase_events"])

    def test_late_bridge_recomputes_and_merges_sessions(self):
        first = [event(0), event(2, 35 * 60_000, kind="purchase")]
        before = reference(first, "scope")["summary"]
        after = reference(first + [event(1, 25 * 60_000, kind="add_to_cart")], "scope")["summary"]
        self.assertEqual((2, 0), (before["total_sessions"], before["purchase_sessions"]))
        self.assertEqual((1, 1), (after["total_sessions"], after["purchase_sessions"]))

    def test_unsorted_input_ties_and_scope_ids_are_deterministic(self):
        rows = [event(0), event(1), event(2, 1, kind="add_to_cart"), event(3, 2, kind="purchase")]
        expected = reference(rows, "scope")
        random.Random(12).shuffle(rows)
        self.assertEqual(expected, reference(rows, "scope"))
        self.assertNotEqual(expected["sessions"], reference(rows, "other")["sessions"])
        self.assertEqual("001", expected["sessions"][0]["visitor_id"])
        first = min(rows[:], key=lambda row: (row["event_time"], row["event_id"]))
        self.assertEqual(session_id("scope", "001", first["event_id"]), expected["sessions"][0]["session_id"])

    def test_empty_input_has_zero_rates_and_an_empty_product_fingerprint(self):
        result = reference([], "empty")
        self.assertEqual(0, result["summary"]["total_sessions"])
        self.assertEqual(0, result["summary"]["product_count"])
        self.assertEqual(hashlib.sha256(b"").hexdigest(), result["summary"]["products_sha256"])
        validate_counts(result["summary"])

    def test_reference_refuses_duplicates_and_unbounded_input(self):
        with self.assertRaisesRegex(ValueError, "deduplicated"):
            reference([event(0), event(0)], "scope")
        with self.assertRaisesRegex(ValueError, "bounded"):
            reference([event(0), event(1)], "scope", max_events=1)

    def test_invalid_funnel_counts_and_rates_are_refused(self):
        for values in ((1, 1, 0, 1, 0), (1, 2, 1, 0, 0), (-1, 0, 0, 0, 0)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                counts(*values)
        row = counts(3, 1, 1, 1, 1)
        for bad in (float("nan"), float("inf"), .5):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate_counts({**row, "view_to_cart_rate": bad})

    def test_product_fingerprint_preserves_ids_and_requires_order(self):
        row = counts(3, 1, 1, 1, 1)
        rows = [{"item_id": "001", **row}, {"item_id": "1", **row}]
        self.assertEqual(2, product_fingerprint(rows)["product_count"])
        for bad in (list(reversed(rows)), [rows[0], rows[0]], [{"item_id": 1, **row}]):
            with self.subTest(rows=bad), self.assertRaises(ValueError):
                product_fingerprint(bad)


if __name__ == "__main__":
    unittest.main()
