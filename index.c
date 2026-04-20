// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>


// object.c APIs used by index_add
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out);
int object_exists(const ObjectID *id);
void object_path(const ObjectID *id, char *path_out, size_t path_size);
// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
//
// Identifies files that are staged, unstaged (modified/deleted in working dir),
// and untracked (present in working dir but not in index).
// Returns 0.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here. 
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

int index_load(Index *index) {
    if (!index) return -1;
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // Missing index file is valid: empty index.
        return 0;
    }

    char line[2048];
    while (fgets(line, sizeof(line), f)) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fclose(f);
            return -1;
        }

        unsigned mode = 0;
        char hex[HASH_HEX_SIZE + 1] = {0};
        unsigned long long mtime = 0;
        unsigned size = 0;
        char path[512] = {0};

        // Format: <mode> <hash> <mtime> <size> <path>
        int n = sscanf(line, "%o %64s %llu %u %511[^\n]",
                       &mode, hex, &mtime, &size, path);
        if (n != 5) {
            fclose(f);
            return -1;
        }

        IndexEntry e;
        memset(&e, 0, sizeof(e));
        e.mode = (uint32_t)mode;
        e.mtime_sec = (uint64_t)mtime;
        e.size = (uint32_t)size;
        strncpy(e.path, path, sizeof(e.path) - 1);
        e.path[sizeof(e.path) - 1] = '\0';

        if (hex_to_hash(hex, &e.hash) != 0) {
            fclose(f);
            return -1;
        }

        index->entries[index->count++] = e;
    }

    fclose(f);
    return 0;
}

static int cmp_index_entries_by_path(const void *a, const void *b) {
    const IndexEntry *ea = (const IndexEntry *)a;
    const IndexEntry *eb = (const IndexEntry *)b;
    return strcmp(ea->path, eb->path);
}

int index_save(const Index *index) {
    if (!index) return -1;

    // Heap copy (avoid stack overflow)
    Index *sorted = (Index *)malloc(sizeof(Index));
    if (!sorted) return -1;
    *sorted = *index;

    qsort(sorted->entries, (size_t)sorted->count, sizeof(IndexEntry), cmp_index_entries_by_path);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", INDEX_FILE);

    FILE *f = fopen(tmp_path, "w");
    if (!f) {
        free(sorted);
        return -1;
    }

    for (int i = 0; i < sorted->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted->entries[i].hash, hex);

        if (fprintf(f, "%06o %s %llu %u %s\n",
                    sorted->entries[i].mode,
                    hex,
                    (unsigned long long)sorted->entries[i].mtime_sec,
                    sorted->entries[i].size,
                    sorted->entries[i].path) < 0) {
            fclose(f);
            unlink(tmp_path);
            free(sorted);
            return -1;
        }
    }

    if (fflush(f) != 0) {
        fclose(f);
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    int fd = fileno(f);
    if (fd < 0 || fsync(fd) != 0) {
        fclose(f);
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    if (fclose(f) != 0) {
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    if (rename(tmp_path, INDEX_FILE) != 0) {
        unlink(tmp_path);
        free(sorted);
        return -1;
    }

    free(sorted);
    return 0;
}

int index_add(Index *index, const char *path) {
    if (!index || !path) return -1;

    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "error: '%s' is not a regular file\n", path);
        return -1;
    }

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long file_size = ftell(f);
    if (file_size < 0) { fclose(f); return -1; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return -1; }

    size_t len = (size_t)file_size;
    void *buf = malloc(len > 0 ? len : 1);
    if (!buf) { fclose(f); return -1; }

    if (len > 0 && fread(buf, 1, len, f) != len) {
        free(buf);
        fclose(f);
        return -1;
    }
    fclose(f);

    ObjectID blob_id;
    if (object_write(OBJ_BLOB, buf, len, &blob_id) != 0) {
        free(buf);
        return -1;
    }
    free(buf);

    IndexEntry *e = index_find(index, path);
    if (!e) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        e = &index->entries[index->count++];
        memset(e, 0, sizeof(*e));
        strncpy(e->path, path, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';
    }

    e->mode = (uint32_t)st.st_mode;
    e->hash = blob_id;
    e->mtime_sec = (uint64_t)st.st_mtime;
    e->size = (uint32_t)st.st_size;

    return index_save(index);
}
