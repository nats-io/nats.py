# Copyright 2018 The NATS Authors
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

import sys
import unittest
from nats.aio.nuid import NUID, MAX_SEQ, PREFIX_LENGTH, TOTAL_LENGTH
from tests.utils import NatsTestCase
from collections import Counter


class NUIDTest(NatsTestCase):
    def setUp(self):
        super().setUp()

    def test_nuid_length(self):
        nuid = NUID()
        self.assertEqual(len(nuid.next()), TOTAL_LENGTH)

    def test_nuid_are_unique(self):
        nuid = NUID()
        entries = [nuid.next().decode() for i in range(500000)]
        counted_entries = Counter(entries)
        repeated = [
            entry for entry, count in counted_entries.items() if count > 1
        ]
        self.assertEqual(len(repeated), 0)

    def test_nuid_are_very_unique(self):
        nuid = NUID()
        entries = [nuid.next().decode() for i in range(1000000)]
        counted_entries = Counter(entries)
        repeated = [
            entry for entry, count in counted_entries.items() if count > 1
        ]
        self.assertEqual(len(repeated), 0)

    def test_nuid_sequence_rollover(self):
        nuid = NUID()
        seq_a = nuid._seq
        inc_a = nuid._inc
        nuid_a = nuid.next()

        seq_b = nuid._seq
        inc_b = nuid._inc
        self.assertTrue(seq_a < seq_b)
        self.assertEqual(seq_b, seq_a + inc_a)
        nuid_b = nuid.next()
        self.assertEqual(nuid_a[:PREFIX_LENGTH], nuid_b[:PREFIX_LENGTH])

        # Force the sequence to rollover, prefix should now change
        nuid._seq = seq_c = MAX_SEQ + 1
        nuid_c = nuid.next()
        self.assertNotEqual(nuid_a[:PREFIX_LENGTH], nuid_c[:PREFIX_LENGTH])


if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
