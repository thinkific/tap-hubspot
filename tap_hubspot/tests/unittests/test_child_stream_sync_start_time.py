import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone

import singer
import tap_hubspot
from tap_hubspot import sync_contact_lists, sync_forms


CONTACT_LISTS_SCHEMA = {
    "type": "object",
    "properties": {
        "listId": {"type": ["null", "string"]},
        "updatedAt": {"type": ["null", "string"], "format": "date-time"},
        "name": {"type": ["null", "string"]},
    }
}

LIST_MEMBERSHIPS_SCHEMA = {
    "type": "object",
    "properties": {
        "recordId": {"type": ["null", "string"]},
        "listId": {"type": ["null", "string"]},
        "membershipTimestamp": {"type": ["null", "string"], "format": "date-time"},
    }
}

FORMS_SCHEMA = {
    "type": "object",
    "properties": {
        "guid": {"type": ["null", "string"]},
        "updatedAt": {"type": ["null", "string"], "format": "date-time"},
    }
}

FORM_SUBMISSIONS_SCHEMA = {
    "type": "object",
    "properties": {
        "conversionId": {"type": ["null", "string"]},
        "formId": {"type": ["null", "string"]},
        "submittedAt": {"type": ["null", "string"], "format": "date-time"},
    }
}

SCHEMAS = {
    "contact_lists": CONTACT_LISTS_SCHEMA,
    "list_memberships": LIST_MEMBERSHIPS_SCHEMA,
    "forms": FORMS_SCHEMA,
    "form_submissions": FORM_SUBMISSIONS_SCHEMA,
}


def make_catalog(stream, schema, key_properties):
    field_metadata = [
        {"breadcrumb": ["properties", prop], "metadata": {"inclusion": "automatic"}}
        for prop in schema["properties"]
    ]
    return {
        "stream": stream,
        "tap_stream_id": stream,
        "stream_alias": None,
        "schema": schema,
        "metadata": [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": key_properties,
                    "forced-replication-method": "INCREMENTAL",
                    "selected": True,
                }
            }
        ] + field_metadata,
    }


CATALOGS = {
    "contact_lists": make_catalog("contact_lists", CONTACT_LISTS_SCHEMA, ["listId"]),
    "list_memberships": make_catalog("list_memberships", LIST_MEMBERSHIPS_SCHEMA, ["recordId", "listId"]),
    "forms": make_catalog("forms", FORMS_SCHEMA, ["guid"]),
    "form_submissions": make_catalog("form_submissions", FORM_SUBMISSIONS_SCHEMA, ["conversionId"]),
}


class MockContext:
    def __init__(self, selected_stream_ids):
        self.selected_stream_ids = selected_stream_ids

    def get_catalog_from_id(self, stream_name):
        return CATALOGS[stream_name]


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data
        self.status_code = 200

    def json(self):
        return self.json_data


class AdvancingClock:
    """Simulates wall-clock time advancing during a long-running sync: every
    utils.now() call returns a timestamp `step` later than the previous one."""

    def __init__(self, start, step_minutes=10):
        self.current = start
        self.step = timedelta(minutes=step_minutes)

    def __call__(self):
        value = self.current
        self.current = self.current + self.step
        return value


class SingerWritePatches:
    """Route write_bookmark into real state handling, capture write_record calls,
    and silence schema/state output."""

    def __init__(self):
        self.written = []

    def __enter__(self):
        self.originals = (singer.write_record, singer.write_schema,
                          singer.write_state, singer.write_bookmark)
        singer.write_record = lambda stream, record, *a, **kw: self.written.append((stream, record))
        singer.write_schema = MagicMock()
        singer.write_state = MagicMock()
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)
        return self

    def __exit__(self, *exc):
        (singer.write_record, singer.write_schema,
         singer.write_state, singer.write_bookmark) = self.originals
        return False

    def records_for(self, stream):
        return [record for name, record in self.written if name == stream]


SYNC_1_START = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
SYNC_2_START = datetime(2024, 6, 2, 0, 0, 0, tzinfo=timezone.utc)

# A membership event that lands while sync 1 is running (after SYNC_1_START).
MID_SYNC_MEMBERSHIP_TS = "2024-06-01T00:25:00.000000Z"

# A list created shortly after sync 1 snapshots the set of lists: absent from
# sync 1's contact_lists response, present in sync 2's. Its members carry
# membershipTimestamps from inside sync 1's execution window.
LIST_CREATED_MID_SYNC_TS = "2024-06-01T00:05:00.000000Z"


class TestListMembershipsMidSyncListCreation(unittest.TestCase):
    """
    Regression tests for AE-320: the list_memberships bookmark must never advance
    past the moment sync_contact_lists snapshotted the set of lists. When
    sync_list_memberships captured utils.now() per list, the bookmark crept
    forward in wall-clock time, permanently dropping members of any list created
    mid-sync.
    """

    def run_contact_lists_sync(self, state, clock, list_pages, membership_pages):
        """membership_pages: one single-page join-order API response body per
        child list, in the order the lists are processed."""
        ctx = MockContext(["contact_lists", "list_memberships"])
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        membership_responses = [
            MockResponse({"results": rows, "paging": {}}) for rows in membership_pages
        ]
        with SingerWritePatches() as writes, \
                patch('tap_hubspot.utils.now', side_effect=clock), \
                patch('tap_hubspot.load_schema', side_effect=SCHEMAS.__getitem__), \
                patch('tap_hubspot.post_search_endpoint', side_effect=[MockResponse(p) for p in list_pages]), \
                patch('tap_hubspot.request', side_effect=membership_responses):
            state = sync_contact_lists(state, ctx)

        return state, writes

    @staticmethod
    def initial_state():
        # Bookmarks present => incremental sync (single descending pass).
        return {
            "currently_syncing": "contact_lists",
            "bookmarks": {
                "contact_lists": {"updatedAt": "2024-01-01T00:00:00.000000Z"},
                "list_memberships": {"membershipTimestamp": "2024-01-01T00:00:00.000000Z"},
            }
        }

    def sync_1(self):
        """Only list A exists in the snapshot; one of its members arrives mid-sync."""
        list_pages = [{
            "lists": [{"listId": "A", "updatedAt": "2024-05-01T00:00:00Z", "name": "List A"}],
            "hasMore": False,
            "offset": 0,
        }]
        membership_pages = [
            [{"recordId": "a1", "membershipTimestamp": MID_SYNC_MEMBERSHIP_TS}],
        ]
        return self.run_contact_lists_sync(
            self.initial_state(), AdvancingClock(SYNC_1_START), list_pages, membership_pages)

    def test_bookmark_not_advanced_past_sync_start(self):
        state, _ = self.sync_1()

        bookmark = singer.bookmarks.get_bookmark(state, "list_memberships", "membershipTimestamp")
        # With the per-list utils.now() bug, the bookmark lands on
        # MID_SYNC_MEMBERSHIP_TS (00:25) because wall-clock time has moved past it.
        self.assertLessEqual(
            singer.utils.strptime_to_utc(bookmark), SYNC_1_START,
            "list_memberships bookmark advanced past the start of the sync")

    def test_members_of_list_created_mid_sync_are_captured_on_next_sync(self):
        state, sync1_writes = self.sync_1()

        # Sanity: sync 1 saw list A's member.
        self.assertIn("a1", [r["recordId"] for r in sync1_writes.records_for("list_memberships")])

        # Sync 2: list B (created at 00:05 during sync 1) now appears in the
        # snapshot, newest first. Its member's membershipTimestamp falls inside
        # sync 1's execution window.
        list_pages = [{
            "lists": [
                {"listId": "B", "updatedAt": LIST_CREATED_MID_SYNC_TS, "name": "List B"},
                {"listId": "A", "updatedAt": "2024-05-01T00:00:00Z", "name": "List A"},
            ],
            "hasMore": False,
            "offset": 0,
        }]
        membership_pages = [
            [{"recordId": "b1", "membershipTimestamp": LIST_CREATED_MID_SYNC_TS}],
            [{"recordId": "a1", "membershipTimestamp": MID_SYNC_MEMBERSHIP_TS}],
        ]
        state["currently_syncing"] = "contact_lists"
        _, sync2_writes = self.run_contact_lists_sync(
            state, AdvancingClock(SYNC_2_START), list_pages, membership_pages)

        written_ids = [r["recordId"] for r in sync2_writes.records_for("list_memberships")]
        self.assertIn(
            "b1", written_ids,
            "member of a list created mid-sync was permanently dropped on the following sync")


class TestChildStreamsReceiveParentSyncStartTime(unittest.TestCase):
    """
    The parents must capture utils.now() exactly once, before snapshotting their
    child-bearing records, and pass that same instant to every child-stream call.
    """

    @patch('tap_hubspot.sync_list_memberships')
    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema', side_effect=SCHEMAS.__getitem__)
    @patch('tap_hubspot.utils.now', side_effect=AdvancingClock(SYNC_1_START))
    def test_contact_lists_passes_single_sync_start_time(
            self, mock_now, mock_load_schema, mock_post, mock_sync_memberships):
        mock_post.return_value = MockResponse({
            "lists": [
                {"listId": "1", "updatedAt": "2024-05-01T00:00:00Z", "name": "One"},
                {"listId": "2", "updatedAt": "2024-04-01T00:00:00Z", "name": "Two"},
            ],
            "hasMore": False,
            "offset": 0,
        })
        mock_sync_memberships.side_effect = lambda *args: (args[1], args[6])

        state = {
            "currently_syncing": "contact_lists",
            "bookmarks": {
                "contact_lists": {"updatedAt": "2024-01-01T00:00:00.000000Z"},
                "list_memberships": {"membershipTimestamp": "2024-01-01T00:00:00.000000Z"},
            }
        }
        ctx = MockContext(["contact_lists", "list_memberships"])
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        with SingerWritePatches():
            sync_contact_lists(state, ctx)

        self.assertEqual(mock_sync_memberships.call_count, 2)
        passed_start_times = {call[0][7] for call in mock_sync_memberships.call_args_list}
        self.assertEqual(
            passed_start_times, {SYNC_1_START},
            "every sync_list_memberships call must receive the sync's single start time")

    @patch('tap_hubspot.sync_form_submissions')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.load_schema', side_effect=SCHEMAS.__getitem__)
    @patch('tap_hubspot.utils.now', side_effect=AdvancingClock(SYNC_1_START))
    def test_forms_passes_single_sync_start_time(
            self, mock_now, mock_load_schema, mock_request, mock_sync_submissions):
        mock_request.return_value = MockResponse([
            {"guid": "f1", "updatedAt": "2024-05-01T00:00:00Z"},
            {"guid": "f2", "updatedAt": "2024-04-01T00:00:00Z"},
        ])
        mock_sync_submissions.side_effect = lambda *args: (args[1], args[6])

        state = {
            "currently_syncing": "forms",
            "bookmarks": {
                "forms": {"updatedAt": "2024-01-01T00:00:00.000000Z"},
                "form_submissions": {"submittedAt": "2024-01-01T00:00:00.000000Z"},
            }
        }
        ctx = MockContext(["forms", "form_submissions"])
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        with SingerWritePatches():
            sync_forms(state, ctx)

        self.assertEqual(mock_sync_submissions.call_count, 2)
        passed_start_times = {call[0][7] for call in mock_sync_submissions.call_args_list}
        # SYNC_1_START is the clock's first tick: the capture must precede the
        # forms API request, so a form created between request and capture can't
        # poison the form_submissions bookmark.
        self.assertEqual(
            passed_start_times, {SYNC_1_START},
            "every sync_form_submissions call must receive the sync's single start time")


if __name__ == '__main__':
    unittest.main()
