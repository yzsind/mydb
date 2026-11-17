"""Negative controls for applied positions and exact, bounded-memory row checks."""
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

import replication_check as check


def status(**changes):
    return dict(Replica_IO_Running='Yes', Replica_SQL_Running='Yes',
                Relay_Source_Log_File='bin.000001', Exec_Source_Log_Pos=100,
                Read_Source_Log_Pos=1000, **changes)


class ReplicationCheckTests(unittest.TestCase):
    def test_received_progress_is_not_applied_progress(self):
        self.assertFalse(check.applied(status(), ('bin.000001', 200)))
        self.assertTrue(check.applied(status(), ('bin.000001', 100)))
        self.assertFalse(check.applied(status(), ('bin.000002', 4)))
        self.assertTrue(check.position_reached('bin.000010', 4, ('bin.000009', 1000)))
        self.assertFalse(check.position_reached('other.000010', 4, ('bin.000009', 1000)))

    def test_thread_errors_and_stopped_appliers_cannot_pass(self):
        for field in ['Last_IO_Error', 'Last_SQL_Error']:
            with self.assertRaises(RuntimeError):
                check.applied(status(**{field: 'broken'}), ('bin.000001', 4))
        stopped = status(); stopped['Replica_SQL_Running'] = 'No'
        self.assertFalse(check.applied(stopped, ('bin.000001', 4)))

    def test_wait_times_out_before_comparison_when_only_io_has_caught_up(self):
        with patch.object(check, 'query', return_value=[status()]), \
             patch.object(check.time, 'monotonic', side_effect=[0, 2]):
            with self.assertRaises(TimeoutError):
                check.wait_applied(Mock(), ('bin.000001', 200), 1)
        for timeout in [0, -1, float('nan'), float('inf')]:
            with self.assertRaises(ValueError):
                check.wait_applied(Mock(), ('bin.000001', 4), timeout)

    def test_wait_requires_one_channel_and_can_observe_later_apply(self):
        with patch.object(check, 'query', side_effect=[[status()], [status()]]), \
             patch.object(check, 'applied', side_effect=[False, True]), patch.object(check.time, 'sleep'):
            self.assertEqual(check.wait_applied(Mock(), ('bin.000001', 100), 1), status())
        for rows in [[], [status(), status()]]:
            with patch.object(check, 'query', return_value=rows), self.assertRaises(ValueError):
                check.wait_applied(Mock(), ('bin.000001', 100), 1)

    def test_physical_progress_must_be_durable_and_healthy(self):
        physical = dict(Durable_Applied_LSN=99, Applied_LSN=100, Last_Status='running', Resync_Required='no', Receiver_Running='yes')
        self.assertFalse(check.applied(physical, 100, True))
        physical['Durable_Applied_LSN'] = 100
        self.assertTrue(check.applied(physical, 100, True))
        self.assertFalse(check.applied(physical | dict(Receiver_Running='no'), 100, True))
        for changes in [dict(Last_Status='stopped: apply failed'), dict(Resync_Required='yes')]:
            with self.assertRaises(RuntimeError):
                check.applied(physical | changes, 100, True)

    def compare(self, left, right, columns=('id', 'value')):
        class Cursor:
            description = [(name,) for name in columns]
            def __init__(self, rows):
                self.rows = iter(rows)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def execute(self, sql):
                pass
            def fetchmany(self, size):
                import itertools
                return list(itertools.islice(self.rows, size))
        source = SimpleNamespace(cursor=lambda *args: Cursor(left))
        replica = SimpleNamespace(cursor=lambda *args: Cursor(right))
        with patch.object(check, 'table_names', return_value=['table']):
            return check.compare_tables(source, replica, 'database')

    def test_equal_counts_and_sums_do_not_hide_changed_rows(self):
        with self.assertRaisesRegex(ValueError, 'typed rows differ'):
            self.compare([(1, 10), (2, 20)], [(1, 20), (2, 10)])

    def test_exact_rows_allow_scan_order_and_decimal_scale_changes(self):
        self.assertEqual(self.compare([(1, Decimal('1.00')), (2, b'\x00\t\xff')],
                                     [(2, b'\x00\t\xff'), (1, Decimal('1.0'))]), {'table': 2})
        self.assertEqual(self.compare([], []), {'table': 0})

    def test_duplicates_nulls_and_numeric_looking_strings_remain_distinct(self):
        for left, right in [([(1, '001')], [(1, '1')]), ([(1, None)], [(1, 'NULL')]),
                            ([(1, 'a'), (1, 'a')], [(1, 'a')]),
                            ([(1, Decimal('1.00001'))], [(1, Decimal('1.00002'))])]:
            with self.subTest(left=left), self.assertRaises(ValueError):
                self.compare(left, right)

    def test_missing_or_empty_table_sets_fail(self):
        for sets in [[[], []], [['a'], ['b']]]:
            with patch.object(check, 'table_names', side_effect=sets), self.assertRaises(ValueError):
                check.compare_tables(Mock(), Mock(), 'db')


if __name__ == '__main__':
    unittest.main()
