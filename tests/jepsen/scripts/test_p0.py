"""Keep semi-sync gate readiness tied to a live, acknowledged WAL frontier."""
import unittest
from unittest.mock import patch

from run_p0 import wait_for_slot_ack


class SlotAckTests(unittest.TestCase):
    def test_waits_for_a_connected_sender_and_the_requested_frontier(self):
        states = [
            {'Connected_Senders': 0, 'Min_Slot_Acked_LSN': 100},
            {'Connected_Senders': 1, 'Min_Slot_Acked_LSN': 41},
            {'Connected_Senders': 1, 'Min_Slot_Acked_LSN': 42},
        ]
        with patch('run_p0.status', side_effect=states), patch('run_p0.time.sleep'):
            self.assertEqual(wait_for_slot_ack(object(), 42), states[-1])

    def test_stalled_ack_fails_with_the_target_and_last_observed_state(self):
        state = {'Connected_Senders': 1, 'Min_Slot_Acked_LSN': 41}
        with patch('run_p0.status', return_value=state), \
             patch('run_p0.time.monotonic', side_effect=[0, 0, 11]), \
             patch('run_p0.time.sleep'):
            with self.assertRaisesRegex(TimeoutError, 'master ACK target=42.*41'):
                wait_for_slot_ack(object(), 42)


if __name__ == '__main__':
    unittest.main()
