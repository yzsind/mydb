#!/usr/bin/env bash
# Configure an Ubuntu development environment for mydb.
# Supported targets: Ubuntu 22.04 LTS, 24.04 LTS, and 26.04 LTS.

# Ubuntu's /bin/sh is normally dash. If the script was invoked as
# `sh scripts/setup_ubuntu.sh`, restart it with Bash before using Bash-only
# syntax such as pipefail and arrays.
if [ -z "${BASH_VERSION:-}" ]; then
  if command -v bash >/dev/null 2>&1; then
    exec bash "$0" "$@"
  fi
  echo "This installer requires Bash. Install bash and run it again." >&2
  exit 1
fi

set -Eeuo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

WITH_BENCH=0
SKIP_BUILD=0
CHINA_MIRROR=0
SKIP_NEXTEST=0

usage() {
  cat <<'EOF'
Usage: scripts/setup_ubuntu.sh [options]

Options:
  --china-mirror  Use RSProxy for rustup and crates.io downloads.
  --with-bench    Install common benchmark helpers (sysbench, Java, Python, jq).
  --skip-nextest  Do not install cargo-nextest; cargo build remains available.
  --skip-build    Install tools only; do not run the initial cargo build.
  -h, --help      Show this help.

Optional nextest environment variables:
  MYDB_NEXTEST_ARCHIVE=/path/to/archive.tar.gz
      Install a pre-downloaded official nextest archive.
  MYDB_NEXTEST_URL=https://mirror.example/archive.tar.gz
      Download nextest from a trusted private or proxy mirror.
  MYDB_DOWNLOAD_TIMEOUT=90
      Per-attempt download timeout in seconds (default: 90).

The script is idempotent and does not read, create, or modify mydb's data/.
Run it as your normal login user; it invokes sudo only for apt packages.
EOF
}

while (($# > 0)); do
  case "$1" in
    --china-mirror)
      CHINA_MIRROR=1
      ;;
    --with-bench)
      WITH_BENCH=1
      ;;
    --skip-nextest)
      SKIP_NEXTEST=1
      ;;
    --skip-build)
      SKIP_BUILD=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ ! -r /etc/os-release ]]; then
  echo "Cannot identify the operating system: /etc/os-release is missing." >&2
  exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" ]]; then
  echo "This installer supports Ubuntu only (detected: ${PRETTY_NAME:-unknown})." >&2
  exit 1
fi

case "${VERSION_ID:-}" in
  22.04|24.04|26.04)
    ;;
  *)
    echo "Warning: Ubuntu ${VERSION_ID:-unknown} is not a regularly tested target; continuing." >&2
    ;;
esac

if ((EUID == 0)) && [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
  echo "Do not run the whole script with sudo; run it as ${SUDO_USER} instead." >&2
  exit 1
fi

if ((EUID == 0)); then
  APT=(env DEBIAN_FRONTEND=noninteractive apt-get)
  PRIVILEGED=()
else
  if ! command -v sudo >/dev/null 2>&1; then
    echo "sudo is required to install Ubuntu packages." >&2
    exit 1
  fi
  APT=(sudo env DEBIAN_FRONTEND=noninteractive apt-get)
  PRIVILEGED=(sudo)
fi

APT_PACKAGES=(
  build-essential
  ca-certificates
  clang
  cmake
  curl
  git
  libssl-dev
  lld
  mysql-client
  pkg-config
)

echo "==> Installing Ubuntu packages"
"${APT[@]}" update
"${APT[@]}" install -y --no-install-recommends "${APT_PACKAGES[@]}"

if ((WITH_BENCH)); then
  if ! apt-cache show sysbench >/dev/null 2>&1; then
    echo "==> Enabling Ubuntu's universe repository for benchmark tools"
    "${APT[@]}" install -y --no-install-recommends software-properties-common
    "${PRIVILEGED[@]}" add-apt-repository -y universe
    "${APT[@]}" update
  fi

  BENCH_PACKAGES=(
    default-jdk-headless
    jq
    numactl
    python3
    python3-venv
    sysbench
    time
  )
  "${APT[@]}" install -y --no-install-recommends "${BENCH_PACKAGES[@]}"
fi

LOGIN_USER="$(id -un)"
LOGIN_HOME="$(getent passwd "${LOGIN_USER}" | cut -d: -f6)"
if [[ -z "${LOGIN_HOME}" ]]; then
  echo "Cannot determine the home directory for ${LOGIN_USER}." >&2
  exit 1
fi

CARGO_ROOT="${CARGO_HOME:-${LOGIN_HOME}/.cargo}"
CARGO_BIN_DIR="${CARGO_ROOT}/bin"
export PATH="${CARGO_BIN_DIR}:${PATH}"

if ((CHINA_MIRROR)); then
  echo "==> Using RSProxy for Rust downloads"
  export RUSTUP_DIST_SERVER="https://rsproxy.cn"
  export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
  export CARGO_REGISTRIES_CRATES_IO_INDEX="sparse+https://rsproxy.cn/index/"
  RUSTUP_INIT_URL="https://rsproxy.cn/rustup-init.sh"
else
  RUSTUP_INIT_URL="https://sh.rustup.rs"
fi

TEMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TEMP_DIR}"
}
trap cleanup EXIT

if ! command -v rustup >/dev/null 2>&1; then
  echo "==> Installing rustup"
  curl --proto '=https' --tlsv1.2 -sSf "${RUSTUP_INIT_URL}" \
    -o "${TEMP_DIR}/rustup-init.sh"
  sh "${TEMP_DIR}/rustup-init.sh" -y --profile minimal --default-toolchain none
fi

echo "==> Installing the stable Rust toolchain"
rustup toolchain install stable --profile minimal --component rustfmt --component clippy
rustup default stable

NEXTEST_AVAILABLE=0
if cargo nextest --version >/dev/null 2>&1; then
  NEXTEST_AVAILABLE=1
elif ((SKIP_NEXTEST)); then
  echo "==> Skipping cargo-nextest installation"
else
  echo "==> Installing cargo-nextest"
  mkdir -p "${CARGO_BIN_DIR}"
  NEXTEST_ARCHIVE="${MYDB_NEXTEST_ARCHIVE:-}"
  if [[ -n "${NEXTEST_ARCHIVE}" ]]; then
    if [[ ! -r "${NEXTEST_ARCHIVE}" ]]; then
      echo "Cannot read MYDB_NEXTEST_ARCHIVE: ${NEXTEST_ARCHIVE}" >&2
      exit 1
    fi
  else
    NEXTEST_URL="${MYDB_NEXTEST_URL:-}"
    if [[ -z "${NEXTEST_URL}" ]]; then
      case "$(uname -m)" in
        x86_64)
          NEXTEST_URL="https://get.nexte.st/0.9/linux"
          ;;
        aarch64|arm64)
          NEXTEST_URL="https://get.nexte.st/0.9/linux-arm"
          ;;
        riscv64)
          NEXTEST_URL="https://get.nexte.st/0.9/linux-riscv64"
          ;;
        *)
          NEXTEST_URL=""
          ;;
      esac
    fi

    if [[ -n "${NEXTEST_URL}" ]]; then
      NEXTEST_ARCHIVE="${TEMP_DIR}/cargo-nextest.tar.gz"
      DOWNLOAD_TIMEOUT="${MYDB_DOWNLOAD_TIMEOUT:-90}"
      echo "    source: ${NEXTEST_URL}"
      if ! curl --proto '=https' --tlsv1.2 -LsSf \
        --connect-timeout 10 --max-time "${DOWNLOAD_TIMEOUT}" \
        --speed-limit 10240 --speed-time 30 --retry 1 --retry-all-errors \
        "${NEXTEST_URL}" -o "${NEXTEST_ARCHIVE}"; then
        echo "Warning: cargo-nextest download failed or was too slow; continuing without it." >&2
        echo "Retry later with --skip-nextest, MYDB_NEXTEST_URL, or" >&2
        echo "MYDB_NEXTEST_ARCHIVE. cargo build is unaffected." >&2
        NEXTEST_ARCHIVE=""
      fi
    else
      echo "Warning: no prebuilt nextest archive is known for $(uname -m); skipping it." >&2
    fi
  fi

  if [[ -n "${NEXTEST_ARCHIVE}" ]]; then
    tar -xzf "${NEXTEST_ARCHIVE}" -C "${CARGO_BIN_DIR}"
    if cargo nextest --version >/dev/null 2>&1; then
      NEXTEST_AVAILABLE=1
    else
      echo "The nextest archive did not install a working cargo-nextest binary." >&2
      exit 1
    fi
  fi
fi

echo "==> Tool versions"
rustc --version
cargo --version
if ((NEXTEST_AVAILABLE)); then
  cargo nextest --version
else
  echo "cargo-nextest: not installed (optional)"
fi
mysql --version

if ((!SKIP_BUILD)); then
  echo "==> Fetching dependencies and building mydb"
  cd "${REPO_ROOT}"
  cargo fetch --locked
  cargo build --locked
  cargo run --locked -- --help >/dev/null
fi

cat <<EOF

mydb development environment is ready.

Start a disposable development server:
  cd ${REPO_ROOT}
  MYDB_DATA_DIR=/tmp/mydb/data cargo run -- --host 127.0.0.1 --port 3307

In another terminal:
  mysql -h127.0.0.1 -P3307 -uroot -proot --ssl-mode=DISABLED test

EOF

if ((NEXTEST_AVAILABLE)); then
  cat <<'EOF'
Run the test suite:
  cargo nextest run
EOF
else
  cat <<'EOF'
Run tests without nextest (slower for this repository):
  cargo test
EOF
fi
