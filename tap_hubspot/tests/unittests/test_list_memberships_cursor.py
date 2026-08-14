import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

import requests
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
    else:
        body["paging"] = {}
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
        catalog = CATALOGS["list_memberships"]
        with SingerWritePatches() as writes, \
                patch('tap_hubspot.utils.now', return_value=SYNC_START), \
                patch('tap_hubspot.request', side_effect=responses) as mock_request:
            state, max_bk = sync_list_memberships(
                list_id, state, LIST_MEMBERSHIPS_SCHEMA, catalog,
                'membershipTimestamp', start, start, SYNC_START)
        return state, writes, mock_request

    def saved_cursors(self, state):
        return singer.bookmarks.get_bookmark(
            state, "list_memberships", LIST_MEMBERSHIPS_CURSOR_KEY)

    def test_bootstrap_pages_through_all_records_and_saves_cursor(self):
        responses = [
            page([member("r1", "2024-02-01T00:00:00Z"),
                  member("r2", "2024-03-01T00:00:00Z")], next_after="CURSOR-1"),
            page([member("r3", "2024-04-01T00:00:00Z")]),
        ]
        state, writes, mock_request = self.run_sync(state_with(), responses)

        # No `after` on the first request; the saved next.after on the second.
        self.assertNotIn("after", mock_request.call_args_list[0][0][1])
        self.assertEqual(mock_request.call_args_list[1][0][1]["after"], "CURSOR-1")

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r1", "r2", "r3"])

        # The most advanced cursor seen is persisted for the next sync.
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-1"})

    def test_resume_starts_from_saved_cursor_and_advances_it(self):
        responses = [
            page([member("r4", "2024-05-01T00:00:00Z")], next_after="CURSOR-2"),
            page([]),
        ]
        state, writes, mock_request = self.run_sync(
            state_with(cursors={"L1": "CURSOR-1"}), responses)

        self.assertEqual(mock_request.call_args_list[0][0][1]["after"], "CURSOR-1")
        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r4"])
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-2"})

    def test_single_page_without_cursor_leaves_no_cursor_to_resume(self):
        responses = [page([member("r1", "2024-02-01T00:00:00Z")])]
        state, _, _ = self.run_sync(state_with(), responses)

        # Nothing to resume from; the next sync rescans this list and relies on
        # the membershipTimestamp filter to avoid re-emitting.
        self.assertEqual(self.saved_cursors(state), {})

    def test_resumed_tail_records_older_than_bookmark_are_not_reemitted(self):
        # The final page is refetched when the API stopped returning next.after:
        # records already emitted last sync sit below the bookmark and stay quiet.
        responses = [
            page([member("r4", "2024-05-01T00:00:00Z"),
                  member("r5", "2024-06-15T00:00:00Z")]),
        ]
        state, writes, _ = self.run_sync(
            state_with(cursors={"L1": "CURSOR-1"}), responses,
            start="2024-06-01T00:00:00.000000Z")

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r5"])
        # Cursor is retained even when the resumed scan returned no new one.
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-1"})

    def test_rejected_cursor_falls_back_to_full_scan(self):
        stale_error = requests.exceptions.HTTPError(
            response=MagicMock(status_code=400))
        responses = [
            stale_error,
            page([member("r1", "2024-02-01T00:00:00Z")], next_after="CURSOR-9"),
            page([]),
        ]
        state, writes, mock_request = self.run_sync(
            state_with(cursors={"L1": "STALE"}), responses)

        # First call used the stale cursor, the fallback rescanned from the top.
        self.assertEqual(mock_request.call_args_list[0][0][1]["after"], "STALE")
        self.assertNotIn("after", mock_request.call_args_list[1][0][1])

        written = [r["recordId"] for r in writes.records_for("list_memberships")]
        self.assertEqual(written, ["r1"])
        self.assertEqual(self.saved_cursors(state), {"L1": "CURSOR-9"})

    def test_non_cursor_http_errors_propagate(self):
        server_error = requests.exceptions.HTTPError(
            response=MagicMock(status_code=500))
        with self.assertRaises(requests.exceptions.HTTPError):
            self.run_sync(state_with(cursors={"L1": "C"}), [server_error])

    def test_cursors_tracked_independently_per_list(self):
        state = state_with(cursors={"L1": "CURSOR-1"})
        responses = [page([member("x1", "2024-05-01T00:00:00Z")], next_after="CURSOR-L2"),
                     page([])]
        state, _, mock_request = self.run_sync(state, responses, list_id="L2")

        # L2 had no cursor: full scan, no `after` on its first request.
        self.assertNotIn("after", mock_request.call_args_list[0][0][1])
        self.assertEqual(self.saved_cursors(state),
                         {"L1": "CURSOR-1", "L2": "CURSOR-L2"})


if __name__ == '__main__':
    unittest.main()
