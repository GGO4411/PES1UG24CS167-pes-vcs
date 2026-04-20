// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    if (!data || !id_out) return -1;

    const char *type_str = NULL;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob";   break;
        case OBJ_TREE:   type_str = "tree";   break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (header_len <= 0 || (size_t)header_len + 1 >= sizeof(header)) return -1;

    size_t full_len = (size_t)header_len + 1 + len; // +1 for '\0'
    unsigned char *full = (unsigned char *)malloc(full_len);
    if (!full) return -1;

    memcpy(full, header, (size_t)header_len);
    full[header_len] = '\0';
    if (len > 0) memcpy(full + header_len + 1, data, len);

    ObjectID id;
    compute_hash(full, full_len, &id);

    // Return hash even if object already exists (dedup)
    *id_out = id;
    if (object_exists(&id)) {
        free(full);
        return 0;
    }

    // Ensure base and shard directories exist
    (void)mkdir(PES_DIR, 0755);
    (void)mkdir(OBJECTS_DIR, 0755);

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    if (mkdir(shard_dir, 0755) != 0) {
        // If mkdir fails and dir does not exist, it's an error
        if (access(shard_dir, F_OK) != 0) {
            free(full);
            return -1;
        }
    }

    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/.tmp-XXXXXX", shard_dir);

    int fd = mkstemp(tmp_path);
    if (fd < 0) {
        free(full);
        return -1;
    }

    size_t written = 0;
    while (written < full_len) {
        ssize_t n = write(fd, full + written, full_len - written);
        if (n <= 0) {
            close(fd);
            unlink(tmp_path);
            free(full);
            return -1;
        }
        written += (size_t)n;
    }

    if (fsync(fd) != 0) {
        close(fd);
        unlink(tmp_path);
        free(full);
        return -1;
    }

    if (close(fd) != 0) {
        unlink(tmp_path);
        free(full);
        return -1;
    }

    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        free(full);
        return -1;
    }

    // fsync shard directory to persist rename
    int dfd = open(shard_dir, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        (void)fsync(dfd);
        close(dfd);
    }

    free(full);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    if (!id || !type_out || !data_out || !len_out) return -1;

    char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return -1; }
    long fsize_l = ftell(f);
    if (fsize_l < 0) { fclose(f); return -1; }
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return -1; }

    size_t fsize = (size_t)fsize_l;
    if (fsize == 0) { fclose(f); return -1; }

    unsigned char *buf = (unsigned char *)malloc(fsize);
    if (!buf) { fclose(f); return -1; }

    size_t nread = fread(buf, 1, fsize, f);
    fclose(f);
    if (nread != fsize) {
        free(buf);
        return -1;
    }

    // Integrity check: hash(file contents) must match requested ID
    ObjectID actual;
    compute_hash(buf, fsize, &actual);
    if (memcmp(actual.hash, id->hash, HASH_SIZE) != 0) {
        free(buf);
        return -1;
    }

    unsigned char *nul = memchr(buf, '\0', fsize);
    if (!nul) {
        free(buf);
        return -1;
    }

    size_t header_len = (size_t)(nul - buf);
    char *header = (char *)malloc(header_len + 1);
    if (!header) {
        free(buf);
        return -1;
    }
    memcpy(header, buf, header_len);
    header[header_len] = '\0';

    char type_str[16];
    size_t declared_len = 0;
    if (sscanf(header, "%15s %zu", type_str, &declared_len) != 2) {
        free(header);
        free(buf);
        return -1;
    }
    free(header);

    ObjectType parsed_type;
    if (strcmp(type_str, "blob") == 0) parsed_type = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) parsed_type = OBJ_TREE;
    else if (strcmp(type_str, "commit") == 0) parsed_type = OBJ_COMMIT;
    else {
        free(buf);
        return -1;
    }

    size_t data_len = fsize - header_len - 1;
    if (data_len != declared_len) {
        free(buf);
        return -1;
    }

    void *out = malloc(data_len ? data_len : 1);
    if (!out) {
        free(buf);
        return -1;
    }
    if (data_len > 0) memcpy(out, nul + 1, data_len);

    *type_out = parsed_type;
    *data_out = out;
    *len_out = data_len;

    free(buf);
    return 0;
}
