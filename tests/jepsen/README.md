# mydb Jepsen: READ COMMITTED

Tests mydb's READ COMMITTED through the real MySQL protocol. A standalone
Clojure project: it adds no Rust runtime dependencies and does not change
Cargo build configuration. It covers single-node fault-free concurrency, plus
process crashes, COMMIT disconnects, and recovery history checks through a
separate entry point. A further local multi-process matrix exercises native
primary/replica faults and fenced promotions. Power loss and other isolation
levels are not covered by these Jepsen entry points.

## Environment and quick start

Requires Java **21 or newer**, Leiningen **2.12.0 or newer**, and Python
**3.11 or newer**. Pinned dependencies: Jepsen 0.3.13, Elle 0.2.7, MySQL
Connector/J 9.4.0. The first run needs access to Maven Central and Clojars to
download dependencies. Both macOS and Linux run directly, with no SSH or
Docker. Graphviz renders anomaly graphs and gnuplot optional performance
plots; the default analysis depends on neither, and text evidence and results
are saved even when anomalies are found.

On macOS, install Leiningen via Homebrew and confirm the commands work:

```bash
brew install leiningen
lein version
java -version
```

If the current terminal still reports `command not found: lein` after
installing, run `rehash` first; if Homebrew's bin directory is not on PATH,
configure your shell as Homebrew suggested and reopen the terminal.

From the repository root:

```bash
cargo build --release --bin mydb
cd tests/jepsen
lein test
cd ../..
python3 tests/jepsen/scripts/run_local.py
```

The script runs `rc-probe`, `append`, `rc-interleave`, and `rc-branches` in
turn, by default 30 seconds each with 8 workers and a target total rate of
100 operations/s. It picks a free loopback port, creates a fresh data
directory, starts the specified binary with strict durability, and stops only
the processes it created on exit, keeping data and logs. It never connects to
an existing database or a remote test server, and never deletes data on its
own.

Point at a custom Leiningen or raise concurrency:

```bash
LEIN=/path/to/lein python3 tests/jepsen/scripts/run_local.py \
  --time-limit 60 --concurrency 32 --rate 300 --seed 42

python3 tests/jepsen/scripts/run_local.py \
  --binary target/release/mydb --workload append \
  --time-limit 60 --concurrency 32 --key-count 4 --rate 300 --seed 7
```

Both local scripts default to `target/release/mydb`; use `--binary` to point
at another build. The binary must be built beforehand; the scripts do not
build it and do not guarantee an existing binary matches the current working
tree. `rc-probe` uses two connections per worker; `append` one. Each
`rc-interleave` worker drives three connections (two writers and one
observer). `rc-branches` opens three connections per case and closes them at
the end, covering the full session lifecycle. `--rate` is the target total
operation rate, not per-worker; one probe spans multiple SQL statements and
transactions.

## Crash-restart and COMMIT disconnects

Run the standalone fault suite from the repository root:

```bash
cargo build --release --bin mydb
python3 -m unittest discover -s tests/jepsen/scripts -p 'test_recovery_proxy.py'
python3 tests/jepsen/scripts/run_recovery.py
```

It uses the same Java, Leiningen, and Python dependencies as the baseline and
manages only the local processes it starts: strict durability, fresh data
directories, no external database addresses. `--binary`, `--lein`, and
`--output` select the build artifact, Leiningen, and a new output directory.

Four scenarios each run from two storage starting points (no explicit
checkpoint / explicit checkpoint after initialization), eight groups in all:

| Scenario | Injection boundary | Recovery requirement |
| --- | --- | --- |
| crash | SIGKILL while both committed and uncommitted transactions exist | commits survive; uncommitted writes vanish |
| before | proxy receives COMMIT but disconnects before forwarding it to the server | client records `:info`; proxy evidence must show no write residue from that transaction |
| after-ok | the server's COMMIT OK reached the proxy, which discards the response and disconnects | client records `:info`; server-side success evidence requires all writes retained |
| inflight | proxy SIGKILLed right after forwarding COMMIT, response never delivered to the client | client records `:info`; the transaction may commit or abort, but the two-row write must be all-or-nothing |

`inflight` does not guarantee hitting a specific internal server instruction
and does not claim every round observes both a commit and an abort. The fault
proxy recognizes COM_QUERY COMMIT at MySQL packet boundaries; this suite
pinned TLS, compression, and server-side prepared statements off. It never
guesses whether a COMMIT succeeded by sleeping.

Each group also includes an explicit rollback transaction and a two-row
uncommitted transaction that survives the crash; the latter writes first,
then lets other transactions commit strictly, covering loser recovery from
the persistent WAL. After recovery, four keys are read back in full, checking
that confirmed commits are not lost, rollbacks leave no residue, unknown
transactions stay atomic, and tokens are not duplicated, and Elle RC runs
over the complete transaction history. A second SIGKILL/recovery then checks
replay idempotency, new commits and rollbacks are executed, and a third
recovery verifies the new data. Every readback and every check must succeed;
fault injection counts, proxy errors, and recovery counts also factor into
the final verdict.

This entry point is a deterministic interleaved fault-history suite built on
Jepsen history and Elle; it does not relax the fault-free `health-checker`'s
strict treatment of `:info`. It is not a randomized multi-node nemesis and
does not simulate power loss or disk failures.

Artifacts land in `/tmp/mydb/jepsen/runs/<timestamp>-recovery-<id>/`:
`manifest.json`, the harness source snapshot `harness/`,
`working-tree.patch`, per-start logs `mydb-*.log`, `console.log`, the
retained `data/`, per-group `history.edn`, `test.edn`, `results.edn`, and the
overall `results.edn` / `results.json`. `results.json` also stores the actual
fault evidence; the exit code is `0` only when all checks pass and every
expected fault actually fired — failures, unknowns, anomalies, and timeouts
are all non-zero. On abnormal interruption, the histories and error records
generated so far are kept.

## Concurrent crashes and native replication faults

The separate Python matrix requires PyMySQL, cryptography and
mysql-connector-python in addition to Java and Leiningen. Use a local virtual
environment under `/tmp/mydb` for Python dependencies. From the repository root:

```bash
python3 -m venv /tmp/mydb/jepsen-venv
/tmp/mydb/jepsen-venv/bin/python -m pip install 'pymysql==1.1.2' cryptography mysql-connector-python
PYTHONDONTWRITEBYTECODE=1 /tmp/mydb/jepsen-venv/bin/python -m unittest discover -s tests/jepsen/scripts -p 'test_*.py'
cargo build --locked --bin mydb
/tmp/mydb/jepsen-venv/bin/python tests/jepsen/scripts/run_fault_matrix.py \
  --binary target/debug/mydb --output /tmp/mydb/jepsen-matrix \
  --seed 20261115 --rounds 1 --operations 16
```

The output directory must be new and below `/tmp/mydb`. The default runs both
`crash` and `cluster` with WAL transaction buffering disabled and enabled.
`--case crash|cluster|semi-sync` and repeated `--buffering 0|1` options select
subsets. Workers are limited to 1–4; each transaction appends unique tokens to
two keys and inserts a typed row in another table, including nullable values,
Decimal, Unicode, binary/LOB, DATETIME, JSON and vector values.

- Crash cases gate concurrent writers after a partial write or before COMMIT,
  perform a checkpoint, release the writers and kill the owned process.
  Readback checks confirmed/aborted/unknown outcomes, cross-table atomicity,
  heap/index equality and recovery followed by new writes. The gate does not
  establish an exact internal COMMIT or checkpoint crash instruction.
- Cluster cases start one primary and two replicas, partition live replication
  connections, heal with delayed/fragmented traffic, restart a replica and the
  primary, perform a source-fenced planned promotion and demotion, then promote
  a lagging candidate only after killing its source. Promotion is followed by
  new writes, a checkpoint and restart. This matrix uses strict local durability,
  asynchronous replication and disabled replication TLS.
- Semi-sync cases (`--case semi-sync`) run the same ledger with
  `MYDB_REPL_MODE=semi_sync` and wait count 1. Acknowledged commits must keep
  advancing while one of two ack replicas is partitioned, degrade to local
  durability (timeout/degraded counters advance, acked counter frozen) while
  every ack replica is cut, survive a primary crash with **no** async-loss
  exemption, and still pass the source-fenced planned promotion and demotion.

Python readback checks run first; `mydb.fault-history` then runs final-state,
unknown-transaction atomicity and Elle RC analysis on the saved histories.
During the forced lagging promotion, only the explicitly recorded acknowledged
transactions after the candidate's unchanged durable-applied boundary may be
lost, and partial transactions remain forbidden. Original history files are
retained. Elle excludes operations containing the allowed-loss append tokens
and reports `:isolation-scope :retained-timeline`; this is a narrower isolation
check than analyzing the complete history.

The matrix saves parameters, binary hash, HEAD, histories, transaction details,
per-scenario results and server logs. Its manifest does not yet contain the
full harness/source snapshot saved by `run_local.py` and `run_recovery.py`.
All nodes share the local host; the matrix does not simulate power loss,
arbitrary disk failures beyond the syscall gates below, or a randomized
multi-host nemesis.

## Syscall I/O fault gates

`run_io.py` compiles `scripts/fixtures/storage_io_fault.c` and injects it with
`DYLD_INSERT_LIBRARIES` on macOS or `LD_PRELOAD` on Linux (the interposer gates
`fsync`, Linux `fdatasync`, `write` and `pwrite`, because mydb syncs the WAL
with `fdatasync` on Linux). Each scenario arms one fault (`fsync-eio`,
`fsync-enospc`, `write-enospc`, `write-short`) against an owned `system/wal` or
`objects/tables` directory with WAL buffering on and off: the SQL error must
surface, the marker must be consumed, recovery must show either the old or the
new full state (never a partial commit), acknowledged commits must survive,
heap and index must agree, and a second restart must be stable.

```bash
/tmp/mydb/jepsen-venv/bin/python tests/jepsen/scripts/run_io.py \
  --binary target/debug/mydb --output /tmp/mydb/jepsen-io
```

## MySQL ROW/FULL receiver regression

`run_mysql_receiver_local.py` owns a fresh local mysqld (ROW/FULL row image and
metadata, GTID on) and drives `scripts/test_mysql_receiver_regressions.py`
against a locally built mydb receiver: crash restarts, XA prepared recovery,
delay, error-1032 handling and GTID/position progress. It needs a mysqld 8.0
binary path; it never attaches to an existing instance.

```bash
/tmp/mydb/jepsen-venv/bin/python tests/jepsen/scripts/run_mysql_receiver_local.py \
  --mysqld /path/to/mysqld --binary target/debug/mydb \
  --output /tmp/mydb/jepsen-receiver
```

The working-tree `.github/workflows/safety.yml` declares all four baseline
workloads, deterministic recovery, the in-crate fuzz regressions
(`cargo nextest run --features fuzzing fuzz`), harness unit tests, this
matrix, and the I/O gates for PR/main/daily runs. Daily matrix runs use three
seeds, five crash rounds and 128 operations per worker plus one semi-sync run;
other events use one seed, one round and 16 operations. A separate
`mysql-receiver` job runs the receiver regression on schedule and manual
dispatch using actions-setup-mysql. These are configured budgets, not evidence
of completed runs. The local coverage audit in
`docs-internal/fuzz-jepsen-coverage-plan-2026-11-15.md` records missing
combinations, checker hardening and acceptance criteria. Development plans
are Git-ignored and are not distributed with a clean checkout.

## Existing instances

Run from the `tests/jepsen` directory:

```bash
JEPSEN_MYDB_PASSWORD=root lein run test \
  --host 127.0.0.1 --port 3307 --db-user root \
  --workload rc-probe --concurrency 8 --time-limit 30 --rate 100

JEPSEN_MYDB_PASSWORD=root lein run test \
  --host 127.0.0.1 --port 3307 --db-user root \
  --workload append --concurrency 32 --time-limit 60 --rate 300 \
  --key-count 16 --max-txn-length 4 --abort-percent 20 --seed 42
```

Each run creates a unique `jepsen_rc_<uuid>` schema and needs
create-database, create-table, and DML privileges; it never reuses or drops
existing schemas. The schema name is stored in `test.jepsen` and also printed
to `console.log`. The password is read only from `JEPSEN_MYDB_PASSWORD`,
defaulting to `root`, and never written into test parameters. `--password`
is Jepsen's own SSH option and is **not** the database password. The
management connection does not use SSH; node lifecycle is managed externally.

The default JDBC `sslMode=DISABLED` suits isolated local testing. For an
existing TLS-enabled instance use
`--jdbc-ssl-mode REQUIRED|VERIFY_CA|VERIFY_IDENTITY`; certificate trust
follows the JVM/Connector/J trust store configuration. Every connection is
explicitly set to RC and the server-side session variable is queried to
confirm.

## Workloads and verdicts

### rc-probe: repeatable two-connection interleaving

Each operation uses a dedicated key; two connections interleave in a
determined order and never guess transaction readiness by sleeping. Each
round carries 17 assertions:

- Uncommitted INSERT, UPDATE, DELETE are invisible to the other connection,
  through both primary-key and secondary-index access.
- The current transaction sees its own updates, deletes, and NULL inserts.
- ROLLBACK restores the original state.
- While an RC transaction stays open, its next SELECT sees versions other
  transactions committed in the meantime.
- A transaction modifying the same row repeatedly never leaks intermediate
  versions or the pre-commit final version to other transactions.
- A transaction modifying two rows checks both rows with a **single SELECT**
  before and after commit, validating the visibility boundary.

SQL, connection, or check exceptions fail the test; unexecuted probes are
never counted as passes. The deterministic assertions here validate the
specified interleavings; they do not exhaust all thread schedules such as
commits landing mid-scan. "The next statement after commit sees the new
version" is an additional statement-snapshot freshness assertion, stronger
than Elle's Adya RC model; its failure must be reported separately and must
not be labeled a G1a/G1b/G1c anomaly.

### rc-interleave: complex transaction interleaving

After the page-lock and monitoring self-lock fixes, all four P1 retest
passes succeeded, including the 8-worker full interleave matrix and
dual-worker heap scans.

```bash
python3 tests/jepsen/scripts/run_local.py --workload rc-interleave \
  --time-limit 30 --concurrency 8 --rate 20 --scan-rows 512
```

17 scenario classes rotate; each worker drives two connections that
**genuinely contend for the same key** on its own key group; workers use
disjoint keys so winners and transaction outcomes can be judged precisely.
Coverage:

| Scenario | Variants and checks |
| --- | --- |
| Concurrent INSERT on the same primary key | first writer commits/rolls back; second writer fails/succeeds after waiting; final uniqueness checked |
| Concurrent INSERT on different primary keys, same unique string key | first writer commits/rolls back; unique-index conflict and abort-without-residue checked |
| DELETE and re-insert of the same key | re-insert succeeds after the delete commits; original row survives when the delete rolls back and the re-insert fails |
| Primary and unique key migrating together, old key reused | migration commits/rolls back; own new key, the other transaction's old key, and both final access paths checked |
| UPDATE re-checks WHERE after waiting for a lock | `v=10` changing to 20 during the wait affects 0 rows; on rollback it increments correctly to 11 |
| DELETE re-checks WHERE after waiting for a lock | a stale condition must not delete; after the blocker rolls back, the matching row is deleted |
| Cross-table transactions | commit/rollback; a single JOIN across partial writes, all-uncommitted, mid-statement unlock, and the next statement |
| Commits during a scan | primary-key index, secondary index, and heap paths; mixed UPDATE/DELETE/INSERT, primary-key moves, index key changes, and NULL writes |

The first six classes each have commit/rollback endings, 12 classes total.
`performance_schema.data_lock_waits` must show the actual wait edge between
the two specified connections before the checker releases the blocker;
merely starting two threads does not count as coverage. A racing INSERT may
return 1062 or mydb's retryable 1213 abort, but the rollback must be
confirmed and the failed transaction verified to leave no write residue; no
automatic retry happens, and unknown COMMITs, timeouts, or other SQL errors
are never accepted as normal endings.

During a scan, a progress user lock is taken once `WHERE` evaluation reaches
a mid id, then the scan waits on a release lock held by the writer. The
observer confirms via `IS_USED_LOCK` that the progress lock belongs to the
scan connection and that the query has not returned yet, and only then
commits the mixed writes. Under streaming execution the outer PROCESSLIST
state may still be `Sending data`, so scan interleaving is not judged by
whether it shows `User lock`. EXPLAIN runs **on the query containing the
synchronization expression itself**, validating the actual plan key; failure
to verify the specified index/heap path fails the test.

An in-progress scan must return the complete old version; the next query in
the same RC transaction must see the complete new version. The cross-table
cases likewise place their synchronization point inside a single JOIN, so
two SELECTs reading different versions across a commit are not misjudged as
atomicity errors. Scan size `--scan-rows` accepts 128..8192, default 512.

This group uses deterministic SQL assertions and a dedicated coverage
checker rather than the list-append Elle model. All 17 scenario classes must
actually run, and each operation's assertion set must be complete and
duplicate-free; missing scenarios, missing boundaries, connection failures,
and dry runs all fail. Short durations or too-low rates can fail on
insufficient coverage. The seed does not change the fixed interleave plans
and does not pin thread scheduling. Each round cleans up its own rows to
bound data growth; histories keep the actual connection ids, wait evidence,
and expected vs. actual outcomes. It remains a bounded test, not a
soak/stability or throughput benchmark.

To isolate an issue, add `--interleave-case scan-heap` (or the scenario name
from the tables above) to run only that scenario. A pass then means only
that scenario passed, not the full 17-class coverage; the option is recorded
in the manifest and the Jepsen test.

### rc-branches: transaction states and protocol branches

```bash
python3 tests/jepsen/scripts/run_local.py --workload rc-branches \
  --time-limit 60 --concurrency 8 --rate 30

# Targeted verification of one protocol or scenario
python3 tests/jepsen/scripts/run_local.py --workload rc-branches \
  --branch-protocol prepared --branch-case prepared-cursor \
  --time-limit 10 --concurrency 2 --rate 5
```

The default `--branch-protocol both` provides **39 scenario variants**: the
data statements of 18 transaction scenario classes run separately through
the text protocol and server-side prepared statements, plus three
prepared-specific cases. Transaction control commands travel via COM_QUERY.
You can select `text` (18 classes) or `prepared` (21 classes).

| Class | Coverage |
| --- | --- |
| SAVEPOINT (3) | rollback of multi-statement modify/insert/delete; later savepoints invalidated; RELEASE does not drop later savepoints; name-reuse boundary cases; row locks retained after rolling back to a savepoint |
| XA (5) | after PREPARE, the original connection is deregistered server-side while locks persist, and another connection COMMITs/ROLLs BACK; ONE PHASE; the own XID appearing/disappearing in RECOVER; error states and duplicate/wrong/completed XIDs; JOIN/RESUME/SUSPEND explicitly unsupported |
| Locking reads and transaction aborts (6) | FOR UPDATE reads the new version after waiting; FOR SHARE compatibility and upgrade waits; NOWAIT rolls back only the current statement; SKIP LOCKED returns and locks the correct subset; 1205 timeout preserves earlier writes, savepoints and locks; 1213 deadlock ends the whole transaction while the connection stays usable |
| Statement failures (3) | a multi-row INSERT failing a unique/check constraint undoes the whole statement, keeps earlier transaction writes, and releases locks newly taken by the failed statement; the transaction can still commit after a syntax or PREPARE error |
| autocommit (1) | switching from off to on commits pending writes; subsequent statements auto-commit |
| prepared re-binding | one PREPARE, three EXECUTEs; BIGINT boundaries, DECIMAL, Unicode/NUL, binary empties, NULL, and microsecond DATETIME; parameter type and NULL bitmap changes; the server-side instance is removed after CLOSE |
| prepared long data | a 32 KiB binary stream via SEND_LONG_DATA; clearParameters triggers RESET; later empty payloads never mix with old content; a large-field UPDATE rolls back; CLOSE |
| prepared cursors | fetch size 2 with confirmed COM_STMT_FETCH; after the first row another connection commits, subsequent FETCHes still use the old snapshot, and after closing the cursor the next statement in the same RC transaction sees the new value |

Server-side prepared mode disables emulation fallback and caching; every
data statement checks the actual JDBC type and verifies the real command path
using **session-level**
`Com_stmt_prepare/execute/close/reset/fetch/send_long_data` counters.
Histories keep the counter deltas; long data is checked and recorded by
length and SHA-256, avoiding repeated large payloads in logs.

FOR UPDATE/FOR SHARE, timeout, and deadlock scenarios must first observe the
real `data_lock_waits` between the target connections. Deadlocks are built
in a determined order A→B then B→A without pinning the victim: both outcomes
are collected, exactly one side must return 1213 while the other updates one
row successfully; the actual victim is then verified for complete rollback,
savepoint invalidation, and connection reusability, and the surviving
transaction for its exact post-commit data. Histories keep both A/B error
codes. 1213 checks full transaction rollback; 1205 and 3572
check statement-rollback semantics with earlier writes retained. No
transparent retries; an expected SQL error counts as a pass only when the
error code and all subsequent data/lock/transaction states match the
assertions.

Every selected variant must run, with assertion sets complete and
duplicate-free; missing protocol counters, the driver falling back to text
execution, skipped variants, unexpected errors, or unknown outcomes all
fail. `--branch-case` accepts the concrete names from the classes above,
such as `savepoint-rollback`, `xa-commit`, `lock-timeout`,
`prepared-long-data`; a targeted pass covers only the selected variant. The
seed does not change the fixed interleave plans and does not pin thread
scheduling.

The scope is single-node RC and the classic-protocol paths listed here; the
XA cases verify connection closure and cross-session completion, while
process-crash recovery is extended by the standalone fault suite.

### append: randomized transactional history + Elle

A fixed number of keys is initialized, each holding a comma-separated list
of integers. Random transactions contain 1 to `--max-txn-length` reads or
appends. Appends use a single `UPDATE ... SET v=CONCAT(v, ?)` with globally
unique tokens — no client-side read-modify-write. By default 20% of
transactions roll back deliberately after completing, bringing dirty reads
and rollback residue into detection range. The same transaction may read and
write the same key repeatedly.

Elle checks the history under **`:read-committed`**, including the
applicable anomalies G1a (aborted read), G1b (intermediate read), and G1c
(circular information flow). Non-repeatable reads, phantoms, or write skew
that RC permits are not treated as serializability violations. The checker's
unit tests include illegal histories and legal non-repeatable-read
histories, so it is not merely "verifying that normal runs go green."

After generation stops, in-flight transactions are awaited and all keys are
read from the database. An independent final-state checker verifies:

- every confirmed append is still present;
- there are no values from explicitly failed transactions or other
  unexplainable values;
- successful appends and concurrent reads actually happened, and the final
  read succeeded.

This workload checks list/element read-write histories and does not claim
coverage of arbitrary SQL, all data types, or predicate isolation. Lists and
full histories grow with run duration, suiting bounded RC testing; key
rotation and segmented analysis for long soak runs are not implemented, so
the current version should not be run continuously for days.

## Failures and unknown outcomes

A `COMMIT` records `:ok` only on success. Deliberate rollbacks record
`:fail`; an SQL exception during execution counts as a definite failure only
when no commit was attempted and the full transaction rollback is confirmed.
Disconnects/timeouts during the commit phase are always `:info` — even a
subsequent successful ROLLBACK cannot prove the earlier COMMIT did not
happen.

There are no transparent automatic retries. Deadlocks and lock timeouts are
allowed concurrency failures; syntax errors, unsupported SQL, and other
unrecognized errors trip the health checker. The current fault-free baseline
also requires zero `:info` outcomes, so a database that loses service cannot
pass on a handful of successful operations. When faults are introduced
later, permitted unknown outcomes and recovery metrics will need their own
definitions.

Jepsen exit codes: `0` pass, `1` fail, `2` unknown, `254` argument error,
`255` checker exception. The local scripts run all selected workloads and
return `1` if any is non-zero; timeouts are recorded as `124` in the
summary. `unknown` and analyzer exceptions are never treated as success.

## Artifacts and review

Local script artifacts default to
`/tmp/mydb/jepsen/runs/<UTC timestamp>-<id>/`; the fault-recovery script uses
`/tmp/mydb/jepsen/runs/<UTC timestamp>-recovery-<id>/`. Both scripts accept
`--output <dir>`. The default directories are temporary — copy results you
need to keep.

Artifact layout of the local workload script:

```text
manifest.json             # git state, binary SHA-256, Java version, run parameters
working-tree.patch        # working-tree diff of tracked files at run time
harness/                  # source snapshot of this test harness
mydb.log
data/                     # full database retained for review
results.json              # per-workload exit codes and total duration
rc-probe/console.log
rc-probe/store/latest/    # history.edn, test.jepsen, results.edn, ...
append/console.log
append/store/latest/      # same; elle/ holds applicable anomaly evidence
```

Artifacts from a direct `lein run test` default to
`/tmp/mydb/jepsen/store/`; override with the JVM property
`-Dmydb.jepsen.store=<dir>`. The Leiningen build cache lives in
`/tmp/mydb/jepsen/target/`. These paths do not depend on symlinks inside the
project directory. Existing Jepsen test files can be re-analyzed:

```bash
lein run analyze --test /absolute/path/to/store/latest
```

Use the file names actually present in the artifacts. The seed reproduces
the operation plan only, not thread scheduling or generator timings;
troubleshooting must keep the full history. Anomalies should be verified by
hand and reduced to a deterministic Rust/protocol regression before fixing.

For throughput, latency, and anomaly graphs, install gnuplot and Graphviz
and add `--plots` to the direct-run command. This is a correctness test with
full history recording, long-list readback, and analysis overhead — its
throughput is not the database's peak performance. Use the repository's
existing OLTP/sysbench tooling for benchmarking.

References: [Jepsen](https://github.com/jepsen-io/jepsen),
[Elle](https://github.com/jepsen-io/elle),
[mydb MVCC and locking](../../docs/mvcc-and-locking.md).
