#include <iostream>
#include <vector>
#include <string_view>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <cstdint>
#include <cstdio>

// Fast hash map for word frequencies
struct HashMap {
    struct Slot {
        uint32_t offset;
        uint32_t length;
        uint32_t count;
    };

    std::vector<Slot> table;
    std::vector<char> arena;
    uint32_t mask;
    uint32_t size;

    HashMap(uint32_t capacity_bits = 20) {
        table.resize(1 << capacity_bits);
        mask = (1 << capacity_bits) - 1;
        size = 0;
        arena.reserve(1024 * 1024 * 16); // 16MB initial arena
    }

    void insert(const char* word, uint32_t len, uint64_t hash) {
        if (size * 2 > table.size()) {
            resize();
        }

        uint32_t idx = hash & mask;
        while (true) {
            Slot& slot = table[idx];
            if (slot.count == 0) {
                slot.offset = arena.size();
                slot.length = len;
                slot.count = 1;
                arena.insert(arena.end(), word, word + len);
                size++;
                return;
            }
            if (slot.length == len && memcmp(&arena[slot.offset], word, len) == 0) {
                slot.count++;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    void resize() {
        std::vector<Slot> new_table(table.size() * 2);
        uint32_t new_mask = new_table.size() - 1;

        for (const auto& slot : table) {
            if (slot.count > 0) {
                uint64_t hash = 14695981039346656037ULL;
                for (uint32_t i = 0; i < slot.length; ++i) {
                    hash = (hash ^ (unsigned char)arena[slot.offset + i]) * 1099511628211ULL;
                }
                uint32_t idx = hash & new_mask;
                while (new_table[idx].count > 0) {
                    idx = (idx + 1) & new_mask;
                }
                new_table[idx] = slot;
            }
        }
        table = std::move(new_table);
        mask = new_mask;
    }
};

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " [input_file] [output_file]\n";
        return 1;
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        perror("open input");
        return 1;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        perror("fstat");
        close(fd);
        return 1;
    }

    if (sb.st_size == 0) {
        FILE* out = fopen(argv[2], "w");
        if (out) fclose(out);
        close(fd);
        return 0;
    }

    const char* data = (const char*)mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return 1;
    }

    bool is_alpha[256] = {false};
    char to_lower[256] = {0};
    for (int i = 0; i < 256; ++i) {
        if (i >= 'a' && i <= 'z') {
            is_alpha[i] = true;
            to_lower[i] = (char)i;
        } else if (i >= 'A' && i <= 'Z') {
            is_alpha[i] = true;
            to_lower[i] = (char)(i + 32);
        }
    }

    HashMap map;
    const char* p = data;
    const char* end = data + sb.st_size;

    char word_buf[4096];
    while (p < end) {
        while (p < end && !is_alpha[(unsigned char)*p]) {
            p++;
        }
        if (p == end) break;

        uint32_t len = 0;
        uint64_t hash = 14695981039346656037ULL;
        while (p < end && is_alpha[(unsigned char)*p]) {
            char c = to_lower[(unsigned char)*p];
            if (len < sizeof(word_buf)) {
                word_buf[len++] = c;
                hash = (hash ^ (unsigned char)c) * 1099511628211ULL;
            }
            p++;
        }
        map.insert(word_buf, len, hash);
    }

    munmap((void*)data, sb.st_size);
    close(fd);

    std::vector<const HashMap::Slot*> entries;
    entries.reserve(map.size);
    for (const auto& slot : map.table) {
        if (slot.count > 0) {
            entries.push_back(&slot);
        }
    }

    std::sort(entries.begin(), entries.end(), [&map](const HashMap::Slot* a, const HashMap::Slot* b) {
        if (a->count != b->count) {
            return a->count > b->count;
        }
        std::string_view sa(&map.arena[a->offset], a->length);
        std::string_view sb(&map.arena[b->offset], b->length);
        return sa < sb;
    });

    FILE* out = fopen(argv[2], "w");
    if (!out) {
        perror("open output");
        return 1;
    }

    char out_buf[65536];
    setvbuf(out, out_buf, _IOFBF, sizeof(out_buf));

    for (const auto* slot : entries) {
        fprintf(out, "%u %.*s\n", slot->count, slot->length, &map.arena[slot->offset]);
    }

    fclose(out);
    return 0;
}