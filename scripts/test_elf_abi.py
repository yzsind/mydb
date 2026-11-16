#!/usr/bin/env python3
"""Regression coverage for release ABI rejection, including weak imports."""
import unittest
from check_elf_abi import inspect_text


class ElfAbiTests(unittest.TestCase):
    def check(self, versions='Name: GLIBC_2.17 Flags: none', dynamic=None, header=None, program=None):
        return inspect_text(header or 'Class: ELF64\nType: DYN\nMachine: Advanced Micro Devices X86-64',
                            versions, dynamic or '(NEEDED) Shared library: [libc.so.6]',
                            program or '[Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]')

    def test_baseline(self):
        self.assertEqual(self.check()['errors'], [])

    def test_weak_new_version_is_rejected(self):
        self.assertIn('unsupported version requirement: GLIBC_2.39',
                      self.check('Name: GLIBC_2.39 Flags: WEAK')['errors'])

    def test_numeric_comparison(self):
        self.assertEqual(self.check('Name: GLIBC_2.9 Flags: none')['errors'], [])
        self.assertTrue(self.check('Name: GLIBC_2.18 Flags: none')['errors'])

    def test_private_and_cpp_requirements(self):
        self.assertTrue(self.check('Name: GLIBC_PRIVATE Flags: none')['errors'])
        self.assertTrue(self.check('Name: GLIBCXX_3.4.21 Flags: none')['errors'])

    def test_wrong_architecture_interpreter_or_search_path(self):
        self.assertTrue(self.check(header='Class: ELF64\nType: DYN\nMachine: AArch64')['errors'])
        self.assertTrue(self.check(program='[Requesting program interpreter: /home/build/ld.so]')['errors'])
        self.assertTrue(self.check(dynamic='(NEEDED) [libc.so.6]\n(RUNPATH) [/home/build]')['errors'])

    def test_unexpected_library_and_static_file(self):
        self.assertTrue(self.check(dynamic='(NEEDED) [libssl.so.3]')['errors'])
        self.assertTrue(self.check(versions='', dynamic='no dynamic section')['errors'])


if __name__ == '__main__':
    unittest.main()
