// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Forward declaration from object.c
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = (size_t)(space - ptr);
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = (uint32_t)strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', (size_t)(end - ptr));
        if (!null_byte) return -1; // Malformed data

        size_t name_len = (size_t)(null_byte - ptr);
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = (tree->count > 0) ? (size_t)tree->count * 296 : 1;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];

        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += (size_t)written + 1; // +1 for '\0'

        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Local index loader used here so tree.c doesn't require linking against index.o
static int load_index_for_tree(Index *index) {
    if (!index) return -1;
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0; // no index yet => empty index

    char line[2048];
    while (fgets(line, sizeof(line), f)) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fclose(f);
            return -1;
        }

        IndexEntry e;
        memset(&e, 0, sizeof(e));

        unsigned mode = 0;
        char hex[HASH_HEX_SIZE + 1] = {0};
        unsigned long long mtime = 0;
        unsigned size = 0;
        char path[512] = {0};

        int n = sscanf(line, "%o %64s %llu %u %511[^\n]",
                       &mode, hex, &mtime, &size, path);
        if (n != 5) {
            fclose(f);
            return -1;
        }

        e.mode = (uint32_t)mode;
        e.mtime_sec = (uint64_t)mtime;
        e.size = (uint32_t)size;
        snprintf(e.path, sizeof(e.path), "%s", path);

        if (hex_to_hash(hex, &e.hash) != 0) {
            fclose(f);
            return -1;
        }

        index->entries[index->count++] = e;
    }

    fclose(f);
    return 0;
}

// recursive helper: build/write one directory level
static int write_tree_level(const Index *index, const char *prefix, ObjectID *out_id) {
    Tree tree;
    memset(&tree, 0, sizeof(tree));

    char seen_dirs[MAX_TREE_ENTRIES][256];
    int seen_count = 0;
    size_t prefix_len = strlen(prefix);

    for (int i = 0; i < index->count; i++) {
        const char *full = index->entries[i].path;
        const char *rel = full;

        if (prefix_len > 0) {
            if (strncmp(full, prefix, prefix_len) != 0) continue;
            if (full[prefix_len] != '/') continue;
            rel = full + prefix_len + 1;
        }

        if (*rel == '\0') continue;

        const char *slash = strchr(rel, '/');

        if (!slash) {
            // File directly under this directory
            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = index->entries[i].mode;
            e->hash = index->entries[i].hash;
            snprintf(e->name, sizeof(e->name), "%s", rel);
        } else {
            // Subdirectory entry
            size_t dlen = (size_t)(slash - rel);
            if (dlen == 0 || dlen >= sizeof(seen_dirs[0])) return -1;

            char dirname[256];
            memcpy(dirname, rel, dlen);
            dirname[dlen] = '\0';

            // Deduplicate dir names at this level
            int already_seen = 0;
            for (int d = 0; d < seen_count; d++) {
                if (strcmp(seen_dirs[d], dirname) == 0) {
                    already_seen = 1;
                    break;
                }
            }
            if (already_seen) continue;

            if (seen_count >= MAX_TREE_ENTRIES) return -1;
            snprintf(seen_dirs[seen_count++], sizeof(seen_dirs[0]), "%s", dirname);

            char child_prefix[1024];
            if (prefix_len == 0) {
                snprintf(child_prefix, sizeof(child_prefix), "%s", dirname);
            } else {
                snprintf(child_prefix, sizeof(child_prefix), "%s/%s", prefix, dirname);
            }

            ObjectID child_id;
            if (write_tree_level(index, child_prefix, &child_id) != 0) return -1;

            if (tree.count >= MAX_TREE_ENTRIES) return -1;
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = MODE_DIR;
            e->hash = child_id;
            snprintf(e->name, sizeof(e->name), "%s", dirname);
        }
    }

    void *raw = NULL;
    size_t raw_len = 0;
    if (tree_serialize(&tree, &raw, &raw_len) != 0) return -1;

    int rc = object_write(OBJ_TREE, raw, raw_len, out_id);
    free(raw);
    return rc;
}

// Build a tree hierarchy from the current index and write all tree objects.
int tree_from_index(ObjectID *id_out) {
    if (!id_out) return -1;

    Index index;
    if (load_index_for_tree(&index) != 0) return -1;

    return write_tree_level(&index, "", id_out);
}
