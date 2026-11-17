"""Protect dump row contents and executable session/DDL comments."""
import io
import unittest

from _sql_dump import statements


class DumpTests(unittest.TestCase):
    def test_quoted_semicolons_comments_backslashes_and_multiline_values(self):
        sql = "-- header\nINSERT INTO t VALUES ('a;\n-- /* b */\nc','it''s',`a;b`);SELECT 'x\\\'y';"
        result = list(statements(io.StringIO(sql)))
        self.assertEqual(len(result), 2)
        self.assertIn("'a;\n-- /* b */\nc'", result[0])
        self.assertEqual(result[1], "SELECT 'x\\\'y'")

    def test_versioned_set_and_view_ddl_are_executed(self):
        sql = "/*!40103 SET TIME_ZONE='+00:00' */;\nCREATE /*!32312 OR REPLACE */ VIEW v AS SELECT 1;\n"
        result = list(statements(io.StringIO(sql)))
        self.assertIn("SET TIME_ZONE='+00:00'", result[0])
        self.assertIn('OR REPLACE', result[1])
        self.assertIn('VIEW v', result[1])

    def test_nonexecutable_comments_and_tokens_inside_literals(self):
        result = list(statements(io.StringIO("/* header\n */ SELECT '/*! do not execute */'; # comment\n")))
        self.assertEqual(result, ["SELECT '/*! do not execute */'"])

    def test_truncation_and_unsupported_delimiters_are_errors(self):
        for sql in ["SELECT 'x;", 'SELECT 1', '/* truncated', '/*! SET x=1', 'DELIMITER $$\n']:
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                list(statements(io.StringIO(sql)))


if __name__ == '__main__':
    unittest.main()
