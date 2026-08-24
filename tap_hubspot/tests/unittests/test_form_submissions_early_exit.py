import unittest
from unittest.mock import patch
from datetime import datetime, timezone

import tap_hubspot
from tap_hubspot import sync_form_submissions

from test_child_stream_sync_start_time import (
    CATALOGS,
    FORM_SUBMISSIONS_SCHEMA,
    MockResponse,
    SingerWritePatches,
)


SYNC_START = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
BOOKMARK = "2024-06-01T00:00:00.000000Z"


def sub(conversion_id, ts):
    return {"conversionId": conversion_id, "submittedAt": ts}


def page(rows, next_after=None):
    body = {"results": rows}
    if next_after:
        body["paging"] = {"next": {"after": next_after}}
    return MockResponse(body)


class TestFormSubmissionsEarlyExit(unittest.TestCase):
    """
    AE-404: the v1 submissions endpoint returns newest-first, so pagination can
    stop at the first record below the bookmark — provided the (undocumented)
    descending order has actually been observed; otherwise degrade to a full scan.
    """

    def run_sync(self, responses, start=BOOKMARK):
        state = {"currently_syncing": "forms",
                 "bookmarks": {"form_submissions": {"submittedAt": start}}}
        response_iter = iter(responses)
        requested = []

        def fake_request(url, params=None):
            requested.append(dict(params or {}))
            return next(response_iter)

        with SingerWritePatches() as writes, \
                patch('tap_hubspot.utils.now', return_value=SYNC_START), \
                patch('tap_hubspot.request', side_effect=fake_request):
            sync_form_submissions(
                "F1", state, FORM_SUBMISSIONS_SCHEMA, CATALOGS["form_submissions"],
                'submittedAt', start, start, SYNC_START)
        return writes, requested

    def emitted(self, writes):
        return [r["conversionId"] for r in writes.records_for("form_submissions")]

    def test_early_exit_stops_pagination_below_bookmark(self):
        responses = [
            page([sub("n2", "2024-06-10T00:00:00Z"),
                  sub("n1", "2024-06-05T00:00:00Z"),
                  sub("old", "2024-01-05T00:00:00Z")], next_after="PAGE2"),
            # page 2 must never be requested
        ]
        writes, requested = self.run_sync(responses)

        self.assertEqual(self.emitted(writes), ["n2", "n1"])
        self.assertEqual(len(requested), 1,
                         "pagination should stop at the first record below the bookmark")

    def test_quiet_form_stops_on_first_page(self):
        # No new submissions: everything is below the bookmark. The scan must
        # confirm one descending step, then stop without fetching page 2.
        responses = [
            page([sub("o2", "2024-01-06T00:00:00Z"),
                  sub("o1", "2024-01-05T00:00:00Z")], next_after="PAGE2"),
        ]
        writes, requested = self.run_sync(responses)

        self.assertEqual(self.emitted(writes), [])
        self.assertEqual(len(requested), 1)

    def test_ascending_order_disables_early_exit(self):
        # If the API ever returns oldest-first, early exit must not fire —
        # otherwise new records at the tail would be silently dropped.
        responses = [
            page([sub("o1", "2024-01-05T00:00:00Z"),
                  sub("n1", "2024-06-05T00:00:00Z")], next_after="PAGE2"),
            page([sub("n2", "2024-06-10T00:00:00Z")]),
        ]
        writes, requested = self.run_sync(responses)

        self.assertEqual(self.emitted(writes), ["n1", "n2"])
        self.assertEqual(len(requested), 2, "ascending order must fall back to a full scan")

    def test_boundary_record_equal_to_bookmark_is_emitted(self):
        responses = [
            page([sub("n1", "2024-06-10T00:00:00Z"),
                  sub("boundary", BOOKMARK)]),
        ]
        writes, _ = self.run_sync(responses)

        self.assertEqual(self.emitted(writes), ["n1", "boundary"])


if __name__ == '__main__':
    unittest.main()
