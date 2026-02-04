# Copyright 2024 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import datetime
import unittest

from nats.js.api import ClusterInfo, RawStreamMsg


class TestPython38ISOParsing(unittest.TestCase):
    """Test suite for ISO 8601 datetime parsing in JetStream API classes.

    This test suite covers the fix for issue #818, which addressed a missing
    _python38_iso_parsing method that caused AttributeError when parsing
    datetime fields in API responses.
    """

    def test_python38_iso_parsing_basic(self):
        """Test basic ISO 8601 timestamp parsing with Z timezone."""
        time_string = "2024-01-15T10:30:45.123456789Z"
        result = ClusterInfo._python38_iso_parsing(time_string)

        # Should replace Z with +00:00 and truncate fractional seconds to 6 digits
        expected = "2024-01-15T10:30:45.123456+00:00"
        self.assertEqual(result, expected)

    def test_python38_iso_parsing_truncates_nanoseconds(self):
        """Test that fractional seconds are truncated to microseconds (6 digits)."""
        time_string = "2024-01-15T10:30:45.123456789Z"
        result = ClusterInfo._python38_iso_parsing(time_string)

        # Nanoseconds (9 digits) should be truncated to microseconds (6 digits)
        self.assertIn(".123456+", result)
        self.assertNotIn(".123456789", result)

    def test_python38_iso_parsing_short_fractional_seconds(self):
        """Test parsing with fewer than 6 fractional second digits."""
        time_string = "2024-01-15T10:30:45.123Z"
        result = ClusterInfo._python38_iso_parsing(time_string)

        # Should keep all 3 digits when less than 6
        expected = "2024-01-15T10:30:45.123+00:00"
        self.assertEqual(result, expected)

    def test_cluster_info_from_response_with_leader_since(self):
        """Test ClusterInfo.from_response parsing leader_since datetime field.

        This test specifically covers the issue #818 where ClusterInfo tried to
        call _python38_iso_parsing but the method wasn't available in the Base class.
        """
        response = {
            "name": "test-cluster",
            "leader": "node-1",
            "leader_since": "2024-01-15T10:30:45.123456789Z"
        }

        cluster_info = ClusterInfo.from_response(response)

        # Verify the datetime was parsed correctly
        self.assertIsNotNone(cluster_info.leader_since)
        self.assertIsInstance(cluster_info.leader_since, datetime.datetime)
        self.assertEqual(cluster_info.leader_since.year, 2024)
        self.assertEqual(cluster_info.leader_since.month, 1)
        self.assertEqual(cluster_info.leader_since.day, 15)
        self.assertEqual(cluster_info.leader_since.hour, 10)
        self.assertEqual(cluster_info.leader_since.minute, 30)
        self.assertEqual(cluster_info.leader_since.second, 45)
        self.assertEqual(cluster_info.leader_since.microsecond, 123456)
        # Should be UTC timezone
        self.assertEqual(cluster_info.leader_since.tzinfo, datetime.timezone.utc)

    def test_cluster_info_from_response_without_leader_since(self):
        """Test ClusterInfo.from_response when leader_since is not present."""
        response = {
            "name": "test-cluster",
            "leader": "node-1"
        }

        cluster_info = ClusterInfo.from_response(response)

        # leader_since should be None when not in response
        self.assertIsNone(cluster_info.leader_since)
        self.assertEqual(cluster_info.name, "test-cluster")
        self.assertEqual(cluster_info.leader, "node-1")

    def test_cluster_info_from_response_with_empty_leader_since(self):
        """Test ClusterInfo.from_response when leader_since is empty."""
        response = {
            "name": "test-cluster",
            "leader": "node-1",
            "leader_since": ""
        }

        cluster_info = ClusterInfo.from_response(response)

        # Empty string should not be parsed, but kept as empty string
        # (not converted to datetime)
        self.assertEqual(cluster_info.leader_since, "")

    def test_raw_stream_msg_from_response_with_time(self):
        """Test RawStreamMsg.from_response parsing time datetime field.

        This verifies that RawStreamMsg can also use the _python38_iso_parsing
        method from the Base class.
        """
        response = {
            "subject": "test.subject",
            "seq": 42,
            "time": "2024-02-01T14:22:33.987654321Z"
        }

        msg = RawStreamMsg.from_response(response)

        # Verify the datetime was parsed correctly
        self.assertIsNotNone(msg.time)
        self.assertIsInstance(msg.time, datetime.datetime)
        self.assertEqual(msg.time.year, 2024)
        self.assertEqual(msg.time.month, 2)
        self.assertEqual(msg.time.day, 1)
        self.assertEqual(msg.time.hour, 14)
        self.assertEqual(msg.time.minute, 22)
        self.assertEqual(msg.time.second, 33)
        # Nanoseconds should be truncated to microseconds
        self.assertEqual(msg.time.microsecond, 987654)
        # Should be UTC timezone
        self.assertEqual(msg.time.tzinfo, datetime.timezone.utc)

    def test_raw_stream_msg_from_response_complete(self):
        """Test RawStreamMsg.from_response with all fields."""
        response = {
            "subject": "test.messages",
            "seq": 100,
            "data": b"test data",
            "hdrs": b"header data",
            "stream": "TEST_STREAM",
            "time": "2024-02-04T12:00:00.000000000Z"
        }

        msg = RawStreamMsg.from_response(response)

        # Verify all fields
        self.assertEqual(msg.subject, "test.messages")
        self.assertEqual(msg.seq, 100)
        self.assertEqual(msg.sequence, 100)  # Test property
        self.assertEqual(msg.data, b"test data")
        self.assertEqual(msg.hdrs, b"header data")
        self.assertEqual(msg.stream, "TEST_STREAM")
        self.assertIsNotNone(msg.time)
        self.assertEqual(msg.time.hour, 12)
        self.assertEqual(msg.time.minute, 0)
        self.assertEqual(msg.time.second, 0)


if __name__ == '__main__':
    unittest.main()
