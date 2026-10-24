/* Test-only macOS interposer. Arm one operation on an owned undo directory. */
#ifndef __APPLE__
#error "storage_io_fault.c requires macOS"
#endif
#define _DARWIN_C_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int consume(int fd, const char *operation, char *mode) {
    const char *root = getenv("MYDB_STORAGE_GATE_ROOT");
    const char *marker = getenv("MYDB_STORAGE_GATE_MARKER");
    if (!root || !marker) return 0;
    char path[PATH_MAX];
    if (fcntl(fd, F_GETPATH, path) != 0) return 0;
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

static int gate_fsync(int fd) {
    char mode[64];
    if (consume(fd, "fsync", mode)) {
        errno = strcmp(mode, "fsync-enospc") == 0 ? ENOSPC : EIO;
        return -1;
    }
    return fsync(fd);
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
    { (const void *)gate_write, (const void *)write }
};
