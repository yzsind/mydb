import unittest
from run_fault_matrix import decode_list, edn, Keyword, validate_outcomes


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tx = {'id': 1, 'writes': [(0, 2), (1, 3)], 'outcome': 'ok', 'detail': ['typed row']}
        self.actual = {0: [2], 1: [3]}
        self.details = {1: ['typed row']}

    def test_confirmed_and_unknown_transactions(self):
        validate_outcomes([self.tx], self.actual, self.details)
        for outcome in ('fail', 'info'):
            validate_outcomes([dict(self.tx, outcome=outcome)], {0: [], 1: []}, {})
        validate_outcomes([dict(self.tx, outcome='info')], self.actual, self.details)

    def test_lost_partial_aborted_duplicate_and_unexplained_writes_fail(self):
        for tx, actual, details in [
            (self.tx, {0: [], 1: []}, {}),
            (self.tx, {0: [2], 1: []}, self.details),
            (dict(self.tx, outcome='info'), {0: [2], 1: []}, self.details),
            (dict(self.tx, outcome='fail'), self.actual, self.details),
            (self.tx, {0: [2, 2], 1: [3]}, self.details),
            (self.tx, {0: [2, 9], 1: [3]}, self.details),
            (self.tx, self.actual, {}),
            (self.tx, self.actual, {1: ['wrong type/value']}),
            (dict(self.tx, unexpected=True), self.actual, self.details),
        ]:
            with self.subTest(tx=tx, actual=actual, details=details), self.assertRaises(AssertionError):
                validate_outcomes([tx], actual, details)

    def test_async_loss_never_excuses_a_partial_or_prefix_loss(self):
        validate_outcomes([self.tx], {0: [], 1: []}, {}, self.tx['writes'])
        for actual, details, allowed in [({0: [2], 1: []}, self.details, self.tx['writes']),
                                         ({0: [], 1: []}, {}, [(0, 2)]),
                                         (self.actual, self.details, [(8, 99)])]:
            with self.subTest(actual=actual, allowed=allowed), self.assertRaises(AssertionError):
                validate_outcomes([self.tx], actual, details, allowed)

    def test_encoding_and_invalid_lists(self):
        self.assertEqual('[{:type :ok :value [[:append 1 2]]}]',
                         edn([{'type': Keyword('ok'), 'value': [[Keyword('append'), 1, 2]]}]))
        self.assertEqual([1, 2], decode_list('1,2,'))
        for raw in (None, '1', '1,1,', 'x,', '0,'):
            with self.subTest(raw=raw), self.assertRaises((AssertionError, ValueError)):
                decode_list(raw)


if __name__ == '__main__':
    unittest.main()
