/* Explicitly loaded local-test instrumentation; never linked into mydb. */
#if !defined(__APPLE__) || !defined(__arm64__)
#error "publication_fault.c requires macOS arm64"
#endif
#define _DARWIN_C_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

struct rule { char target[32]; char action[64]; };
static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t trace_lock = PTHREAD_MUTEX_INITIALIZER;
static uint64_t rollback_txn, progress_lsn, undo_head, heap_total, heap_after;
static uint64_t progress_device, progress_inode;
static int progress_written;
static unsigned progress_count, recovery_renames;
static _Atomic unsigned observed_rename, observed_links;

static int selected_device(const struct stat *st) {
    const char *device = getenv("MYDB_PUBLICATION_FAULT_DEVICE");
    return !device || (uint64_t)st->st_dev == strtoull(device, NULL, 10);
}

static int rule_load(struct rule *r) {
    const char *marker = getenv("MYDB_PUBLICATION_MARKER");
    if (!marker) return 0;
    int fd = open(marker, O_RDONLY);
    if (fd < 0) return 0;
    char bytes[128];
    ssize_t n = read(fd, bytes, sizeof(bytes) - 1);
    close(fd);
    if (n <= 0) return 0;
    bytes[n] = 0;
    return sscanf(bytes, "%31s %63s", r->target, r->action) == 2;
}

static int scoped(const char *path) {
    const char *root = getenv("MYDB_PUBLICATION_ROOT");
    if (!root || !*root) return 0;
    size_t n = strlen(root);
    return strncmp(path, root, n) == 0 && (path[n] == '/' || path[n] == 0);
}

static int fd_path(int fd, char *path) {
    return fcntl(fd, F_GETPATH, path) == 0 && scoped(path);
}

static int suffix(const char *text, const char *end) {
    size_t n = strlen(text), m = strlen(end);
    return n >= m && memcmp(text + n - m, end, m) == 0;
}

static int target(const struct rule *r, const char *path, int directory) {
    if (!scoped(path)) return 0;
    if (!strcmp(r->target, "clock"))
        return directory ? suffix(path, "/system") :
            strstr(path, "/system/.source-clock-") != NULL || suffix(path, "/system/commit-clock.v1");
    if (!strcmp(r->target, "manifest"))
        return directory ? suffix(path, "/system/undo") :
            suffix(path, "/undo.manifest") || suffix(path, "/undo.manifest.tmp");
    if (!strcmp(r->target, "recovery"))
        return strstr(path, "/system/undo/undo-") != NULL;
    if (!strcmp(r->target, "rollback"))
        return strstr(path, "/system/wal/") != NULL;
    if (!strcmp(r->target, "promotion"))
        return directory ? suffix(path, "/system/replication") :
            suffix(path, "/system/replication/role.json") || suffix(path, "/system/replication/role.json.tmp");
    if (!strcmp(r->target, "link")) return directory || strstr(path, "/objects/") != NULL;
    return 0;
}

static void json_string(int fd, const char *s) {
    dprintf(fd, "\"");
    for (; *s; ++s) {
        if (*s == '"' || *s == '\\') dprintf(fd, "\\%c", *s);
        else if ((unsigned char)*s < 32) dprintf(fd, "\\u%04x", (unsigned char)*s);
        else dprintf(fd, "%c", *s);
    }
    dprintf(fd, "\"");
}

static void trace(const char *op, const char *path, int result) {
    const char *output = getenv("MYDB_PUBLICATION_TRACE");
    if (!output || !scoped(path)) return;
    pthread_mutex_lock(&trace_lock);
    int fd = open(output, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0) _exit(123);
    struct stat st;
    unsigned long long device = stat(path, &st) == 0 ? (unsigned long long)st.st_dev : 0;
    dprintf(fd, "{\"op\":"); json_string(fd, op);
    dprintf(fd, ",\"path\":"); json_string(fd, path);
    dprintf(fd, ",\"result\":%d,\"device\":%llu}\n", result, device);
    close(fd);
    pthread_mutex_unlock(&trace_lock);
}

static int event(const struct rule *r, const char *path, const char *stage, int pause) {
    const char *marker = getenv("MYDB_PUBLICATION_MARKER");
    const char *hit = getenv("MYDB_PUBLICATION_HIT");
    if (!marker || !hit || unlink(marker) != 0) return 0;
    int fd = open(hit, O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0) _exit(122);
    dprintf(fd, "{\"pid\":%d,\"target\":", getpid()); json_string(fd, r->target);
    dprintf(fd, ",\"action\":"); json_string(fd, r->action);
    dprintf(fd, ",\"path\":"); json_string(fd, path);
    dprintf(fd, ",\"stage\":"); json_string(fd, stage);
    dprintf(fd, ",\"txn\":%llu,\"progress_lsn\":%llu,\"undo_head\":%llu,"
            "\"progress_count\":%u,\"heap_total\":%llu,\"heap_after\":%llu,"
            "\"recovery_renames\":%u}\n", (unsigned long long)rollback_txn,
            (unsigned long long)progress_lsn, (unsigned long long)undo_head, progress_count,
            (unsigned long long)heap_total, (unsigned long long)heap_after, recovery_renames);
    fsync(fd); close(fd);
    if (pause) kill(getpid(), SIGSTOP);
    return 1;
}

int publication_before_fcntl(int fd, int command) {
    struct rule r; char path[PATH_MAX]; struct stat st;
    if ((command != F_FULLFSYNC && command != F_BARRIERFSYNC) ||
        !rule_load(&r) || !fd_path(fd, path) || fstat(fd, &st) != 0) return 0;
    if (!strcmp(r.target, "rollback") && !strcmp(r.action, "boundary") &&
        command == F_FULLFSYNC && ((S_ISDIR(st.st_mode) && suffix(path, "/system/wal")) ||
                                 (S_ISREG(st.st_mode) && suffix(path, ".log")))) {
        pthread_mutex_lock(&state_lock);
        int same_file = progress_written && progress_device == (uint64_t)st.st_dev &&
                        progress_inode == (uint64_t)st.st_ino;
        if (progress_count == 1 && undo_head && heap_after == 0 &&
            (S_ISDIR(st.st_mode) || same_file)) {
            int result = fcntl(fd, command);
            if (result != 0) { pthread_mutex_unlock(&state_lock); return 1; }
            event(&r, path, S_ISDIR(st.st_mode) ? "after-wal-publication-directory-fullsync" :
                  "after-progress-inode-fullsync", 1);
            pthread_mutex_unlock(&state_lock);
            return 2;
        }
        pthread_mutex_unlock(&state_lock);
    }
    int directory = S_ISDIR(st.st_mode);
    if (!target(&r, path, directory)) return 0;
    if (!strcmp(r.target, "promotion") && !strcmp(r.action, "intent-after") &&
        directory && observed_rename && command == F_FULLFSYNC) {
        int result = fcntl(fd, command);
        if (result != 0) return 1;
        event(&r, path, "after-promotion-intent-directory-fullsync", 1);
        return 2;
    }
    /* Keep the rule armed until the unsupported barrier's full-sync fallback. */
    if (!strcmp(r.action, "fallback-fullsync-eio") && !directory && command == F_BARRIERFSYNC) {
        errno = ENOTSUP; return 1;
    }
    int selected = (!strcmp(r.action, "barrier-eio") && !directory && command == F_BARRIERFSYNC) ||
                   (!strcmp(r.action, "fallback-fullsync-eio") && !directory && command == F_FULLFSYNC) ||
                   (!strcmp(r.action, "fullsync-eio") && !directory && command == F_FULLFSYNC) ||
                   (!strcmp(r.action, "publish-dirsync-eio") && directory && observed_links && command == F_FULLFSYNC) ||
                   (!strcmp(r.action, "dirsync-eio") && directory && command == F_FULLFSYNC);
    if (selected && selected_device(&st) && event(&r, path, directory ? "before-directory-fullsync" :
            command == F_BARRIERFSYNC ? "before-barrier" : "before-fullsync", 0)) { errno = EIO; return 1; }
    if ((!strcmp(r.action, "observe") || !strcmp(r.action, "batch-observe")) && target(&r, path, directory)) {
        int result = fcntl(fd, command);
        int saved_errno = errno;
        trace(command == F_BARRIERFSYNC ? "barrier" : directory ? "dir-fullsync" : "fullsync", path, result);
        if (!strcmp(r.action, "observe") && directory && result == 0 && observed_rename)
            event(&r, path, "after-observed-directory-sync", 0);
        errno = saved_errno;
        return result == 0 ? 2 : 1;
    }
    return 0;
}

static int gate_fsync(int fd) {
    struct rule r; char path[PATH_MAX]; struct stat st;
    if (rule_load(&r) && !strcmp(r.action, "batch-observe") && fd_path(fd, path) &&
        fstat(fd, &st) == 0 && target(&r, path, S_ISDIR(st.st_mode))) {
        int result = fsync(fd);
        int saved_errno = errno;
        trace("fsync", path, result);
        errno = saved_errno;
        return result;
    }
    if (rule_load(&r) && fd_path(fd, path) && target(&r, path, 0) &&
        (!strcmp(r.action, "fsync-eio") || !strcmp(r.action, "fsync-enospc")) &&
        fstat(fd, &st) == 0 && selected_device(&st) &&
        event(&r, path, "before-fsync", 0)) {
        errno = !strcmp(r.action, "fsync-enospc") ? ENOSPC : EIO;
        return -1;
    }
    return fsync(fd);
}

static ssize_t gate_write(int fd, const void *bytes, size_t size) {
    struct rule r; char path[PATH_MAX];
    if (rule_load(&r) && fd_path(fd, path) && target(&r, path, 0)) {
        if (!strcmp(r.action, "observe")) {
            ssize_t result = write(fd, bytes, size);
            int saved_errno = errno;
            trace("write", path, (int)result);
            errno = saved_errno;
            return result;
        }
        if (!strncmp(r.action, "write-", 6) && event(&r, path, "before-write", 0)) {
            if (!strcmp(r.action, "write-short")) return write(fd, bytes, size > 7 ? 7 : size);
            errno = !strcmp(r.action, "write-enospc") ? ENOSPC : EIO;
            return -1;
        }
    }
    return write(fd, bytes, size);
}

static uint16_t u16(const unsigned char *p) { uint16_t x; memcpy(&x, p, 2); return x; }
static uint32_t u32(const unsigned char *p) { uint32_t x; memcpy(&x, p, 4); return x; }
static uint64_t u64(const unsigned char *p) { uint64_t x; memcpy(&x, p, 8); return x; }

static ssize_t gate_pwrite(int fd, const void *bytes, size_t size, off_t offset) {
    struct rule r; char path[PATH_MAX];
    if (!rule_load(&r) || strcmp(r.target, "rollback") || !fd_path(fd, path) || !target(&r, path, 0))
        return pwrite(fd, bytes, size, offset);
    pthread_mutex_lock(&state_lock);
    const unsigned char *p = bytes;
    size_t cursor = 0, chosen = size;
    int stop = 0, wrote_progress = 0;
    while (size - cursor >= 48 && u32(p + cursor) == 0x4d57414c) {
        size_t frame = 48 + (size_t)u32(p + cursor + 4);
        if (frame > size - cursor) break;
        uint16_t kind = u16(p + cursor + 32);
        uint64_t txn = u64(p + cursor + 24);
        if (kind == 3) rollback_txn = txn;
        if (kind == 32 && frame >= 88) {
            rollback_txn = txn;
            progress_lsn = u64(p + cursor + 8);
            undo_head = u64(p + cursor + 48);
            ++progress_count; heap_after = 0;
            wrote_progress = 1;
        }
        if ((kind == 13 || kind == 20) && txn == 0) {
            ++heap_total;
            if (progress_count) ++heap_after;
        }
        cursor += frame;
        if (!strcmp(r.action, "inside") && progress_count == 1 && heap_after == 13) {
            chosen = cursor; stop = 1; break;
        }
    }
    ssize_t result = pwrite(fd, bytes, chosen, offset);
    if (wrote_progress && result == (ssize_t)chosen) {
        struct stat st;
        if (fstat(fd, &st) == 0) {
            progress_device = (uint64_t)st.st_dev;
            progress_inode = (uint64_t)st.st_ino;
            progress_written = 1;
        }
    }
    if (stop && result == (ssize_t)chosen && fcntl(fd, F_FULLFSYNC) == 0)
        event(&r, path, "after-13-compensation-frames-fullsync", 1);
    pthread_mutex_unlock(&state_lock);
    return result;
}

static int gate_rename(const char *from, const char *to) {
    struct rule r;
    if (!rule_load(&r) || !target(&r, to, 0)) return rename(from, to);
    if (!strcmp(r.target, "recovery") && !suffix(from, ".seg.compacting")) return rename(from, to);
    if (!strcmp(r.action, "rename-before")) event(&r, to, "before-rename", 1);
    if (!strcmp(r.action, "rename-eio") && event(&r, to, "before-rename", 0)) { errno = EIO; return -1; }
    int result = rename(from, to);
    if (result == 0) {
        if (!strcmp(r.target, "promotion")) observed_rename = 1;
        if (!strcmp(r.action, "observe")) { observed_rename = 1; trace("rename", to, result); }
        if (!strcmp(r.target, "recovery") && suffix(from, ".seg.compacting")) ++recovery_renames;
        if (!strcmp(r.action, "rename-after")) event(&r, to, "after-rename", 1);
    }
    return result;
}

static int gate_linkat(int fromfd, const char *from, int tofd, const char *to, int flags) {
    struct rule r;
    if (rule_load(&r) && !strcmp(r.target, "link") && target(&r, to, 0)) {
        if (!strcmp(r.action, "link-eio") && event(&r, to, "before-link", 0)) { errno = EIO; return -1; }
        if (!strcmp(r.action, "link-before")) event(&r, to, "before-link", 1);
        int result = linkat(fromfd, from, tofd, to, flags);
        if (result == 0) ++observed_links;
        if (!strcmp(r.action, "batch-observe")) trace("link", to, result);
        if (result == 0 && !strcmp(r.action, "link-after")) event(&r, to, "after-link", 1);
        return result;
    }
    return linkat(fromfd, from, tofd, to, flags);
}

extern int publication_fcntl(int, int, ...);
__attribute__((used, section("__DATA,__interpose")))
static const struct { const void *replacement; const void *original; } interpositions[] = {
    { (const void *)publication_fcntl, (const void *)fcntl },
    { (const void *)gate_write, (const void *)write },
    { (const void *)gate_pwrite, (const void *)pwrite },
    { (const void *)gate_fsync, (const void *)fsync },
    { (const void *)gate_rename, (const void *)rename },
    { (const void *)gate_linkat, (const void *)linkat }
};
