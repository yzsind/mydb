import datetime
import decimal
import unittest

from clickbench_results import decode, encode, parse_workload, split_top


class ResultFormatTest(unittest.TestCase):
    def test_typed_values_round_trip(self):
        for value in [None, "text\twith\nlines Я", b"\x00\xff", 2**64 - 1,
                      decimal.Decimal("12345678901234567890.1234"),
                      datetime.datetime(2013, 7, 1, 12, 30), datetime.date(2013, 7, 1)]:
            self.assertEqual(decode(encode(value)), value)

    def test_group_alias_and_quoted_commas(self):
        sql = "SELECT UserID, EXTRACT(minute FROM EventTime) AS m, COUNT(*) AS c FROM hits WHERE URL LIKE '%a,b%' GROUP BY UserID, m ORDER BY c DESC LIMIT 10;"
        _, fields, groups, ordering, _, part = parse_workload(sql)
        self.assertEqual(groups, [0, 1])
        self.assertEqual(ordering, [2])
        self.assertEqual(fields[1], "EXTRACT(minute FROM EventTime)")
        self.assertEqual(part("WHERE"), "URL LIKE '%a,b%'")
        self.assertEqual(split_top("1, CONCAT(a, ',b'), c"), ["1", "CONCAT(a, ',b')", "c"])


if __name__ == "__main__":
    unittest.main()
