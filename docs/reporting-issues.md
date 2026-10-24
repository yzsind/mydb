# Reporting Issues

Report bugs at [MyDB GitHub Issues](https://github.com/yzsind/mydb/issues).
Search existing issues first, then open one issue per problem with a descriptive
title. Reports may be written in English or Chinese.

## Required information

Every bug report must include:

- **Exact MyDB version:** release version and package or container image used.
  For a source build, include the Git commit (`git rev-parse HEAD`), build command,
  enabled Cargo features, and any local changes. Include `SELECT VERSION();`
  output if the server can accept connections; otherwise use the package or build
  information. Do not describe the version only as "latest".
- **Operating system:** OS name and version, kernel version, and CPU architecture.
  On Linux, include `cat /etc/os-release` and `uname -a`; on macOS, include
  `sw_vers` and `uname -a`. For Docker, WSL, or a virtual machine, describe both
  the host and guest/container environment. Include CPU, memory, and storage
  details when relevant to resource or performance problems.
- **Configuration and client:** exact server startup command, relevant
  configuration and environment variables, and client/driver name and version.
  Include session settings that affect the reproduction, such as SQL mode,
  character set, and transaction isolation level.
- **Logs and errors:** server startup logs and logs around the failure, with
  timestamps; complete client error messages, error codes, and SQLSTATE when
  available. Include panic messages and backtraces for crashes. Paste text in
  fenced code blocks or attach log files. If no log was produced, state that
  explicitly and include the process output and exit status if available.
- **Reproduction steps and scripts:** numbered steps and a minimal, complete SQL
  file or runnable script that reproduces the problem, including setup, data,
  commands, and dependencies. Include the exact command used to run it.
- **Expected and actual results:** show both results and describe how often the
  problem occurs, including whether it also occurs after restarting the server
  with a fresh test data directory.

Remove passwords, tokens, connection secrets, and private data before submitting.
Use synthetic data that preserves the behavior being reported.

## Make the reproduction self-contained

Use a separate test instance and data directory. Include `CREATE TABLE`, indexes,
data inserts or a deterministic data generator, and the failing SQL in execution
order. Avoid relying on existing tables, private datasets, or undocumented setup.

For transaction or concurrency problems, label each connection and show the exact
statement order, transaction boundaries, and synchronization between sessions.
For restart or recovery problems, include shutdown/crash and restart steps and
explain which data directory is reused. For client or protocol problems, include
the client program and dependency versions.

For performance problems, also include dataset size, query parameters, execution
plans, concurrency, warm-up, measurement commands, and measured timings. If
comparing with MySQL, provide its exact version, configuration, and results for
the same reproduction.

## Issue template

Copy this template into the issue body and replace each placeholder. Attach the
reproduction scripts and logs if they are too large to include inline.

````markdown
### Summary
Describe the problem and its impact.

### MyDB version
Release/package/container image:
Git commit, build command, Cargo features, and local changes (source builds):
SELECT VERSION() output (if available):

### Environment
OS name/version, kernel, and CPU architecture:
Container/VM/WSL and host details (if applicable):
CPU, memory, and storage (if relevant):
Client/driver and version:
Server startup command and relevant configuration/environment variables:
Relevant session settings:

### Reproduction steps
1. Set up the test instance and data.
2. Run the attached SQL file or script with this exact command: ...
3. Observe the failure.

### Reproduction SQL or script
```sql
-- Include schema, indexes, test data, session settings, and failing statements.
```
Script dependencies and execution command:
Connection ordering/synchronization or restart steps (if applicable):
Reproduction frequency and result with a fresh test data directory:

### Expected result
Describe the expected behavior and include the expected output.

### Actual result
Include the actual output and complete error message/code/SQLSTATE.

### Logs
```text
Paste server startup and failure logs, client output, and any panic/backtrace.
```
If logs are unavailable, explain why and include process output/exit status.

### Additional context
Execution plans, performance measurements, comparison results, or workarounds.
````
