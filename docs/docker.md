# Docker Build and Deployment

The repository root provides the local image build files. The image enables TLS,
`fast-alloc`, and `zstd-compression` by default, and the database process runs as
the non-root user with UID/GID `10001:10001`. It contains the server, backup CLI,
SQL client, timezone data, and third-party license/source notices:

```bash
docker build -t mydb:local .

docker volume create mydb-data
docker run --name mydb \
  -p 127.0.0.1:8738:8738 \
  -v mydb-data:/var/lib/mydb \
  -e MYDB_ROOT_PASSWORD='change-me' \
  mydb:local

docker exec -it mydb mysql -h127.0.0.1 -P8738 -uroot -p --ssl
```

Enter the configured initial password at the prompt. The initial administrator
is `root@localhost`; Docker bridge/NAT clients are not loopback clients, so a
published port does not authorize an external root login. From the container's
loopback connection, create a dedicated account for the published endpoint:

```sql
CREATE USER 'app'@'%' IDENTIFIED BY 'choose-a-separate-password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON test.* TO 'app'@'%';
```

```bash
mysql -h127.0.0.1 -P8738 -uapp -p --ssl-mode=REQUIRED test -e 'SELECT 1'
docker exec mydb mydb-backup --help
```

The SQL client bundled by Debian also supports `--ssl`; an external MySQL 8.0
client uses `--ssl-mode`. These first-start examples use the ephemeral local
certificate. Remote deployments should install a trusted certificate and use
identity verification as described in [configuration](configuration.md).

To build a Linux AMD64 image on an Apple Silicon Mac:

```bash
docker buildx build \
  --platform linux/amd64 \
  --progress=plain \
  -t registry.example.com/namespace/mydb:1.0.0 \
  --load \
  .
```

Docker builds support `linux/amd64` and `linux/arm64` and require BuildKit
(enabled by default in current Docker Desktop). The compile stage uses the Rust
slim image for the host architecture and a C cross toolchain for the target
architecture, avoiding emulating another CPU to run the Rust compiler; the
release configuration and features stay unchanged. Cargo downloads and `target`
build artifacts are reused through BuildKit cache mounts, so dependency
compilation results can be reused after source changes; after clearing the
builder cache they must be downloaded and compiled again.

Build concurrency defaults to one Cargo job to limit peak compiler memory on
Docker Desktop. With more memory available, use
`--build-arg CARGO_BUILD_JOBS=4` to increase parallelism. This does not change
the release optimization settings or enabled features. If the build reports
`cannot allocate memory` or rustc is killed with `SIGKILL`, check the Docker
VM's memory allocation and other running workloads.

The current first-distribution candidate is limited to the locally verified
amd64 image. Other architectures retain source-build instructions but are not
shipped in this candidate.

`--progress=plain` distinguishes image download, system dependency installation,
and Cargo download/compile durations. The first build still downloads the Rust
toolchain image and the target standard library; if it stays on the
`sha256:... MB / ... MB` line of `FROM` for a long time, the bottleneck is image
pulling — check the Docker Desktop proxy or configure a trusted image mirror
service. The `HTTPS_PROXY` argument of the build command affects only build
steps and cannot replace the Docker Desktop image pull proxy. With an internal
copy of the images, the image sources can be replaced via
`--build-arg RUST_IMAGE=<internal image address>` and
`--build-arg RUNTIME_IMAGE=<internal image address>`; they respectively keep the
contents of Rust `1.95.0-slim-bookworm` (including the build machine
architecture) and Debian `bookworm-slim` (including the target architecture).
`--load` loads the result into the local daemon; use `--push` instead when
pushing directly to a registry.

The image automatically reads `/etc/mydb/mydb.cnf`. A complete config file can
be read-only mounted at that path to replace the image's default configuration:

```bash
chmod 0644 ./mydb.cnf
docker run --name mydb \
  -p 127.0.0.1:8738:8738 \
  -v mydb-data:/var/lib/mydb \
  -v ./mydb.cnf:/etc/mydb/mydb.cnf:ro \
  -e MYDB_ROOT_PASSWORD='change-me' \
  mydb:local
```

A custom configuration in a container must listen on `0.0.0.0` and should set
`data_dir` to `/var/lib/mydb`; otherwise the port may be unreachable from
outside the container or the data may not be written to the declared volume.
The config file must be a plain UTF-8 file and must not allow group/other
writes; `0644` is the recommended permission. To use another path inside the
container, pass `--config` explicitly; that argument disables the automatic
loading of the fixed path:

```bash
docker run --rm \
  -v ./custom.cnf:/run/mydb/custom.cnf:ro \
  mydb:local --config /run/mydb/custom.cnf
```

All existing `MYDB_*` environment variables can be passed directly via `-e` or
`--env-file`. For example:

```bash
docker run --name mydb \
  -p 127.0.0.1:8738:8738 \
  -v mydb-data:/var/lib/mydb \
  --env-file ./mydb.env \
  -e MYDB_MEMORY_TARGET=1G \
  mydb:local --slow-sql-ms 250
```

Configuration precedence inside the container is: image default or mounted
config < `MYDB_*` environment variables < CLI arguments in the container
command. A named volume is the recommended default; if you bind mount a host
data directory, that directory must be writable by UID/GID `10001:10001`.
On macOS Docker Desktop, keep database and backup repositories on Linux named
volumes. This release review reproduced transient hard-link lookup failures on
shared host folders; those folders are outside the accepted storage path.
The database retains its generation/EOF checks and fails an affected backup
instead of publishing it.
`docker stop` sends `SIGTERM`, and mydb runs its existing connection drain,
checkpoint, and graceful shutdown procedures.


[Back to product introduction](../README.md) · [Full configuration reference](configuration.md)
