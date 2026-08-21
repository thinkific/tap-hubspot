import unittest
from unittest.mock import patch
from datetime import datetime, timezone

import singer
import tap_hubspot
from tap_hubspot import sync_list_memberships, LIST_MEMBERSHIPS_CURSOR_KEY

from test_child_stream_sync_start_time import (
    CATALOGS,
    LIST_MEMBERSHIPS_SCHEMA,
    MockResponse,
    SingerWritePatches,
)


SYNC_START = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
OLD_BOOKMARK = "2024-01-01T00:00:00.000000Z"


def member(record_id, ts):
    return {"recordId": record_id, "membershipTimestamp": ts}


def page(rows, next_after=None):
    body = {"results": rows}
    if next_after:
        body["paging"] = {"next": {"after": next_after}}
    return MockResponse(body)


def state_with(cursors=None):
    bookmarks = {"list_memberships": {"membershipTimestamp": OLD_BOOKMARK}}
    if cursors is not None:
        bookmarks["list_memberships"][LIST_MEMBERSHIPS_CURSOR_KEY] = cursors
    return {"currently_syncing": "contact_lists", "bookmarks": bookmarks}


class TestListMembershipsJoinOrderCursor(unittest.TestCase):
    """
    list_memberships uses the /memberships/join-order endpoint with a per-list
    `after` cursor persisted in state, so subsequent syncs fetch only members
    added since the last run instead of rescanning every page of every list.
    """

    def run_sync(self, state, responses, list_id="L1", start=OLD_BOOKMARK):
        """Returns (state, writes, requested_params) where requested_params is a
        snapshot of the params dict at each request — get_v3_records mutates the
        dict in place between pages, so the live call_args can't be asserted on."""
        catalog = CATALOGS["list_memberships"]
        response_iter = iter(responses)
        requested_params = []

        def fake_request(url, params=None):
            requested_params.append(dict(params or {}))
            response = next(response_iter)
            if isinstance(response, Exception):
                raise response
            return response

        with SingerWritePatches() as writes, \
                patch('tap_hubspot.utils.now', return_value=SYNC_START), \
                patch('tap_hubspot.request', side_effect=fake_request):
            state, _ = sync_list_memberships(
                list_id, state, LIST_MEMBERSHIPS_SCHEMA, catalog,
                'membershipTimestamp', start, start, SYNC_START)
        return state, writes, requested_params

    def saved_cursors(self, state):
        return singer.bookmarks.get_bookmark(
            state, "list_memberships", LIST_MEMBERSHIPS_CURSOR_KEY)

    def test_bootstrap_pages_through_all_records_and_saves_cursor(self):
        responses = [
            page([member("r1", "2024-02-01T00:00:00Z"),
                  member("r2", "2024-03-01T00:00:00Z")], next_after="CURSOR-1"),
            page([member("r3", "2024-04-01T00:00:00Z")]),
        ]
        state, writes, requested = self.run_sync(state_with(), responses)

        # No `after` on the first request; the saved next.after on the second.
        self.assertNotIn("after", requested[0])
        self.assertEqual(requested[1]["after"], "CURSOR-1")

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r1", "r2", "r3"])

        # The most advanced cursor seen is persisted for the next sync.
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-1"})

    def test_resume_starts_from_saved_cursor_and_advances_it(self):
        responses = [
            page([member("r4", "2024-05-01T00:00:00Z")], next_after="CURSOR-2"),
            page([]),
        ]
        state, writes, requested = self.run_sync(
            state_with(cursors={"L1": "CURSOR-1"}), responses)

        self.assertEqual(requested[0]["after"], "CURSOR-1")
        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r4"])
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-2"})

    def test_single_page_without_cursor_leaves_no_cursor_to_resume(self):
        responses = [page([member("r1", "2024-02-01T00:00:00Z")])]
        state, _, _ = self.run_sync(state_with(), responses)

        # Nothing to resume from; the next sync rescans this list and relies on
        # the membershipTimestamp filter to avoid re-emitting.
        self.assertEqual(self.saved_cursors(state), {})

    def test_resumed_scan_emits_records_even_below_the_bookmark(self):
        # Everything a cursor-resumed scan returns joined after the saved
        # position, so it is emitted even when the shared stream bookmark has
        # run ahead of this list (an interrupted sync advances the bookmark off
        # other lists' progress before this list gets its turn). The refetched
        # tail page re-emits at most ~250 records, which the target upserts.
        responses = [
            page([member("r4", "2024-05-01T00:00:00Z"),
                  member("r5", "2024-06-15T00:00:00Z")]),
        ]
        state, writes, _ = self.run_sync(
            state_with(cursors={"L1": "CURSOR-1"}), responses,
            start="2024-06-01T00:00:00.000000Z")

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r4", "r5"])
        # Cursor is retained even when the resumed scan returned no new one.
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-1"})

    def test_full_scan_still_filters_below_bookmark(self):
        # Without a cursor the whole list is refetched every sync; the bookmark
        # filter is what stops those records from being re-emitted each time.
        responses = [
            page([member("r1", "2024-05-01T00:00:00Z"),
                  member("r2", "2024-06-15T00:00:00Z")]),
        ]
        _, writes, _ = self.run_sync(
            state_with(), responses, start="2024-06-01T00:00:00.000000Z")

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r2"])

    def test_failed_cursor_resume_falls_back_to_full_scan(self):
        # request() surfaces exhausted retries as a bare Exception (on_giveup),
        # which is what a stale/rejected cursor looks like from the sync's side.
        giveup = Exception("Giving up on request after 5 tries")
        responses = [
            giveup,
            page([member("r1", "2024-02-01T00:00:00Z")], next_after="CURSOR-9"),
            page([]),
        ]
        state, writes, requested = self.run_sync(
            state_with(cursors={"L1": "STALE"}), responses)

        # First call used the stale cursor, the fallback rescanned from the top.
        self.assertEqual(requested[0]["after"], "STALE")
        self.assertNotIn("after", requested[1])

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r1"])
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-9"})

    def test_unreadable_list_is_skipped_and_cursor_preserved(self):
        # A 403 for a list whose object type the token can't read (company /
        # custom-object lists) surfaces as SourceUnavailableException. It must
        # skip only that list — not abort the parent stream — and keep any
        # stored cursor for when the scope is granted.
        responses = [tap_hubspot.SourceUnavailableException(b'{"status":"error"}')]
        state, writes, _ = self.run_sync(
            state_with(cursors={"L1": "CURSOR-1"}), responses)

        self.assertEqual(writes.records_for("list_memberships"), [])
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-1"})

    def test_unreadable_list_without_cursor_is_skipped(self):
        responses = [tap_hubspot.SourceUnavailableException(b'{"status":"error"}')]
        state, writes, _ = self.run_sync(state_with(), responses)

        self.assertEqual(writes.records_for("list_memberships"), [])
        self.assertEqual(self.saved_cursors(state), {})

    def test_errors_without_stored_cursor_propagate(self):
        with self.assertRaises(Exception):
            self.run_sync(state_with(), [Exception("Giving up on request")])

    def test_error_in_fallback_scan_propagates(self):
        # A genuine outage fails the resumed scan AND the fallback rescan; the
        # second failure must propagate rather than loop.
        responses = [Exception("Giving up on request"), Exception("Giving up on request")]
        with self.assertRaises(Exception):
            self.run_sync(state_with(cursors={"L1": "C"}), responses)

    def test_cursors_tracked_independently_per_list(self):
        state = state_with(cursors={"L1": "CURSOR-1"})
        responses = [page([member("x1", "2024-05-01T00:00:00Z")], next_after="CURSOR-L2"),
                     page([])]
        state, _, requested = self.run_sync(state, responses, list_id="L2")

        # L2 had no cursor: full scan, no `after` on its first request.
        self.assertNotIn("after", requested[0])
        self.assertEqual(self.saved_cursors(state),
                         {"L1": "CURSOR-1", "L2": "CURSOR-L2"})


if __name__ == '__main__':
    unittest.main()
