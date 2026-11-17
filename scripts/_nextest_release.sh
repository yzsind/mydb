#!/usr/bin/env bash
# Select and verify immutable upstream archives before installing executables.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/nextest-release.env"

mydb_nextest_asset() {
  local target
  case "$1" in
    x86_64) target=x86_64-unknown-linux-musl; NEXTEST_SHA256="$NEXT_X86_64_SHA256" ;;
    aarch64|arm64) target=aarch64-unknown-linux-musl; NEXTEST_SHA256="$NEXT_AARCH64_SHA256" ;;
    riscv64) target=riscv64gc-unknown-linux-gnu; NEXTEST_SHA256="$NEXT_RISCV64_SHA256" ;;
    *) NEXTEST_SHA256=""; NEXTEST_URL=""; return 1 ;;
  esac
  NEXTEST_URL="https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${NEXT_VERSION}/cargo-nextest-${NEXT_VERSION}-${target}.tar.gz"
}

mydb_verify_nextest_archive() {
  local archive="$1" expected="$2" digest
  if command -v sha256sum >/dev/null 2>&1; then
    digest="$(sha256sum "$archive")" || return 1
  else
    digest="$(shasum -a 256 "$archive")" || return 1
  fi
  if [[ "${digest%% *}" != "$expected" ]]; then
    echo "cargo-nextest archive SHA-256 mismatch: $archive" >&2
    return 1
  fi
}
