/* Test-only interposer. Arm one operation on an owned storage directory.
 * macOS injects it through DYLD_INSERT_LIBRARIES (__DATA,__interpose);
 * Linux injects it through LD_PRELOAD (dlsym RTLD_NEXT resolves the real
 * functions). mydb syncs the WAL with fdatasync on Linux and fsync on
 * macOS, so both sync entry points are gated. */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE
#elif defined(__linux__)
#define _GNU_SOURCE
#else
#error "storage_io_fault.c requires macOS or Linux"
#endif
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#if defined(__linux__)
#include <dlfcn.h>
#endif

/* Resolve the path behind `fd`; returns NULL when unavailable. */
static const char *fd_path(int fd, char buffer[PATH_MAX]) {
#if defined(__APPLE__)
    return fcntl(fd, F_GETPATH, buffer) == 0 ? buffer : NULL;
#else
    char link[32];
    snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    ssize_t length = readlink(link, buffer, PATH_MAX - 1);
    if (length <= 0)
        return NULL;
    buffer[length] = '\0';
    return buffer;
#endif
}

static int consume(int fd, const char *operation, char *mode) {
    const char *root = getenv("MYDB_STORAGE_GATE_ROOT");
    const char *marker = getenv("MYDB_STORAGE_GATE_MARKER");
    if (!root || !marker) return 0;
    char path[PATH_MAX];
    if (!fd_path(fd, path)) return 0;
    size_t length = strlen(root);
    if (strncmp(path, root, length) != 0 || path[length] != '/') return 0;
    int input = open(marker, O_RDONLY);
    if (input < 0) return 0;
    ssize_t count = read(input, mode, 63);
    close(input);
    if (count <= 0) return 0;
    mode[count] = 0;
    size_t op_length = strlen(operation);
    if (strncmp(mode, operation, op_length) != 0 || mode[op_length] != '-') return 0;
    if (unlink(marker) != 0) return 0;
    dprintf(STDERR_FILENO, "[storage-gate] injected %s into %s\n", mode, path);
    return 1;
}

static int fail_sync(char *mode) {
    errno = strcmp(mode, "fsync-enospc") == 0 ? ENOSPC : EIO;
    return -1;
}

#if defined(__linux__)
static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);

/* Fails one armed write: either a 7-byte short write forwarded to the real
 * entry point, or ENOSPC. `positional` selects pwrite vs write semantics. */
static ssize_t fail_write(ssize_t (*positional)(int, const void *, size_t, off_t),
                          ssize_t (*plain)(int, const void *, size_t),
                          int fd, const void *bytes, size_t size, off_t offset, char *mode) {
    if (strcmp(mode, "write-short") == 0) {
        size_t written = size > 7 ? 7 : size;
        return positional ? positional(fd, bytes, written, offset) : plain(fd, bytes, written);
    }
    errno = ENOSPC;
    return -1;
}

__attribute__((constructor))
static void resolve_real_symbols(void) {
    real_fsync = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = (int (*)(int))dlsym(RTLD_NEXT, "fdatasync");
    real_write = (ssize_t (*)(int, const void *, size_t))dlsym(RTLD_NEXT, "write");
    real_pwrite = (ssize_t (*)(int, const void *, size_t, off_t))dlsym(RTLD_NEXT, "pwrite");
}

int fsync(int fd) {
    char mode[64];
    if (consume(fd, "fsync", mode)) return fail_sync(mode);
    if (!real_fsync) { errno = ENOSYS; return -1; }
    return real_fsync(fd);
}

int fdatasync(int fd) {
    char mode[64];
    if (consume(fd, "fsync", mode)) return fail_sync(mode);
    if (!real_fdatasync) { errno = ENOSYS; return -1; }
    return real_fdatasync(fd);
}

ssize_t write(int fd, const void *bytes, size_t size) {
    char mode[64];
    if (consume(fd, "write", mode))
        return fail_write(NULL, real_write, fd, bytes, size, 0, mode);
    if (!real_write) { errno = ENOSYS; return -1; }
    return real_write(fd, bytes, size);
}

ssize_t pwrite(int fd, const void *bytes, size_t size, off_t offset) {
    char mode[64];
    if (consume(fd, "write", mode))
        return fail_write(real_pwrite, NULL, fd, bytes, size, offset, mode);
    if (!real_pwrite) { errno = ENOSYS; return -1; }
    return real_pwrite(fd, bytes, size, offset);
}

/* glibc exports pwrite64 as an alias of pwrite on 64-bit targets; route it
 * through the same gate so libc::pwrite callers cannot bypass the wrapper. */
ssize_t pwrite64(int fd, const void *bytes, size_t size, off_t offset) {
    return pwrite(fd, bytes, size, offset);
}
#else
static int gate_fsync(int fd) {
    char mode[64];
    if (consume(fd, "fsync", mode)) return fail_sync(mode);
    return fsync(fd);
}

static ssize_t gate_pwrite(int fd, const void *bytes, size_t size, off_t offset) {
    char mode[64];
    if (consume(fd, "write", mode)) {
        if (strcmp(mode, "write-short") == 0)
            return pwrite(fd, bytes, size > 7 ? 7 : size, offset);
        errno = ENOSPC;
        return -1;
    }
    return pwrite(fd, bytes, size, offset);
}

static ssize_t gate_write(int fd, const void *bytes, size_t size) {
    char mode[64];
    if (consume(fd, "write", mode)) {
        if (strcmp(mode, "write-short") == 0)
            return write(fd, bytes, size > 7 ? 7 : size);
        errno = ENOSPC;
        return -1;
    }
    return write(fd, bytes, size);
}

__attribute__((used, section("__DATA,__interpose")))
static const struct { const void *replacement; const void *original; } interpositions[] = {
    { (const void *)gate_fsync, (const void *)fsync },
    { (const void *)gate_write, (const void *)write },
    { (const void *)gate_pwrite, (const void *)pwrite }
};
#endif
