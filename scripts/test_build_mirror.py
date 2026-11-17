"""An optional archive mirror changes only CentOS repository URLs, not trust."""
from pathlib import Path
import tempfile
import sys
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import build_linux_glibc217 as builder


class BuildMirrorTests(unittest.TestCase):
    def test_archive_mirror_preserves_package_signature_and_key_configuration(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary)
            repo = root / 'CentOS-Base.repo'
            repo.write_text('[base]\nbaseurl=https://vault.centos.org/centos/$releasever/os/$basearch/\n'
                            'gpgcheck=1\ngpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7\n'
                            '[updates]\nbaseurl=https://vault.centos.org/7.9.2009/updates/$basearch/\n')
            builder.configure_centos_vault_mirror('https://mirrors.example/centos-vault/7.9.2009/', root)
            result = repo.read_text()
            self.assertIn('baseurl=https://mirrors.example/centos-vault/7.9.2009/os/$basearch/', result)
            self.assertIn('baseurl=https://mirrors.example/centos-vault/7.9.2009/updates/$basearch/', result)
            self.assertIn('gpgcheck=1', result)
            self.assertIn('gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7', result)

    def test_insecure_or_credential_bearing_mirror_urls_are_rejected(self):
        for url in ['http://mirror.example/7', 'https://u:p@mirror.example/7',
                    'https://mirror.example/7?token=x', 'https://mirror.example/7#fragment']:
            with self.subTest(url=url), self.assertRaises(ValueError):
                builder.centos_vault_mirror(url)

    def test_unknown_repository_layout_fails_instead_of_silently_ignoring_mirror(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            with self.assertRaisesRegex(RuntimeError, 'vault'):
                builder.configure_centos_vault_mirror('https://mirrors.example/7', Path(temporary))

    def test_sidecars_fail_before_downloads_and_container_start(self):
        with tempfile.TemporaryDirectory(dir='/tmp/mydb') as temporary:
            root = Path(temporary); source = root / 'source'; source.mkdir()
            for name in ('Cargo.toml', 'Cargo.lock', 'build.rs', 'rust-toolchain.toml'):
                (source / name).write_text('fixture')
            (source / '.config').mkdir(); (source / '.config/._nextest.toml').write_bytes(b'metadata')
            with patch.object(sys, 'argv', ['builder', '--source', str(source), '--output', str(root/'build'), '--test-archive']), \
                 patch.object(builder, 'download_nextest') as download, patch.object(builder, 'call') as call:
                with self.assertRaisesRegex(SystemExit, 'metadata sidecars'):
                    builder.main()
                download.assert_not_called(); call.assert_not_called()

    def test_perl_install_keeps_signature_verification_enabled(self):
        probe = SimpleNamespace(returncode=1, stdout='', stderr='missing module')
        installed = SimpleNamespace(returncode=0, stdout='Perl ready', stderr='')
        with patch.object(builder.subprocess, 'run', side_effect=[probe, installed]), \
             patch.object(builder.shutil, 'which', return_value='/usr/bin/yum'), \
             patch.object(builder, 'configure_centos_vault_mirror') as configure, \
             patch.object(builder, 'call') as call:
            builder.ensure_openssl_build_tools('https://mirrors.example/7')
        configure.assert_called_once_with('https://mirrors.example/7')
        command = call.call_args.args[0]
        self.assertIn('--setopt=gpgcheck=1', command)
        self.assertIn('--disablerepo=*', command)
        self.assertEqual([arg for arg in command if arg.startswith('--enablerepo=')],
                         ['--enablerepo=base', '--enablerepo=updates', '--enablerepo=extras'])


if __name__ == '__main__':
    unittest.main()
