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
#include <thread>
#include <mutex>

#if __has_include(<execution>)
#include <execution>
#define HAS_EXECUTION 1
#endif

// Fast hash map for word frequencies
struct HashMap {
    struct Slot {
        uint32_t offset;
        uint32_t length;
        uint32_t count;
        uint64_t hash; // Store hash to speed up resizing and merging
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
                slot.hash = hash;
                arena.insert(arena.end(), word, word + len);
                size++;
                return;
            }
            if (slot.length == len && slot.hash == hash && memcmp(&arena[slot.offset], word, len) == 0) {
                slot.count++;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    // Used for merging another thread's map into the global map
    void insert_or_add(const char* word, uint32_t len, uint64_t hash, uint32_t count) {
        if (size * 2 > table.size()) {
            resize();
        }

        uint32_t idx = hash & mask;
        while (true) {
            Slot& slot = table[idx];
            if (slot.count == 0) {
                slot.offset = arena.size();
                slot.length = len;
                slot.count = count;
                slot.hash = hash;
                arena.insert(arena.end(), word, word + len);
                size++;
                return;
            }
            if (slot.length == len && slot.hash == hash && memcmp(&arena[slot.offset], word, len) == 0) {
                slot.count += count;
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
                uint32_t idx = slot.hash & new_mask;
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

bool is_alpha_table[256] = {false};
char to_lower_table[256] = {0};

void init_tables() {
    for (int i = 0; i < 256; ++i) {
        if (i >= 'a' && i <= 'z') {
            is_alpha_table[i] = true;
            to_lower_table[i] = (char)i;
        } else if (i >= 'A' && i <= 'Z') {
            is_alpha_table[i] = true;
            to_lower_table[i] = (char)(i + 32);
        }
    }
}

void process_chunk(const char* data, size_t file_size, size_t start_offset, size_t end_offset, HashMap& local_map) {
    const char* p = data + start_offset;
    const char* end = data + end_offset;
    const char* file_end = data + file_size;

    // Adjust start: if we are in the middle of a word, skip it (the previous thread will handle it)
    if (start_offset > 0) {
        if (is_alpha_table[(unsigned char)*(p - 1)]) {
            while (p < file_end && is_alpha_table[(unsigned char)*p]) {
                p++;
            }
        }
    }

    char word_buf[4096];
    
    while (p < end) {
        // Skip non-alpha
        while (p < file_end && !is_alpha_table[(unsigned char)*p]) {
            p++;
        }
        
        // If we reached the end of our chunk before finding a word, stop
        if (p >= end) {
            break;
        }

        uint32_t len = 0;
        uint64_t hash = 14695981039346656037ULL;
        
        // Read word (can go beyond 'end' up to 'file_end' to finish the word)
        while (p < file_end && is_alpha_table[(unsigned char)*p]) {
            char c = to_lower_table[(unsigned char)*p];
            if (len < sizeof(word_buf)) {
                word_buf[len++] = c;
                hash = (hash ^ (unsigned char)c) * 1099511628211ULL;
            }
            p++;
        }
        
        if (len > 0) {
            local_map.insert(word_buf, len, hash);
        }
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " [input_file] [output_file]\n";
        return 1;
    }

    init_tables();

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

    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;
    
    // Fallback to 1 thread for very small files
    if (sb.st_size < 1024 * 1024) {
        num_threads = 1;
    }

    std::vector<std::thread> threads;
    std::vector<HashMap> local_maps(num_threads);

    size_t chunk_size = sb.st_size / num_threads;

    for (unsigned int i = 0; i < num_threads; ++i) {
        size_t start_offset = i * chunk_size;
        size_t end_offset = (i == num_threads - 1) ? sb.st_size : (i + 1) * chunk_size;
        
        threads.emplace_back(process_chunk, data, sb.st_size, start_offset, end_offset, std::ref(local_maps[i]));
    }

    for (auto& t : threads) {
        t.join();
    }

    // Merge local maps into a global one
    HashMap global_map;
    for (auto& local_map : local_maps) {
        for (const auto& slot : local_map.table) {
            if (slot.count > 0) {
                global_map.insert_or_add(&local_map.arena[slot.offset], slot.length, slot.hash, slot.count);
            }
        }
    }

    munmap((void*)data, sb.st_size);
    close(fd);

    std::vector<const HashMap::Slot*> entries;
    entries.reserve(global_map.size);
    for (const auto& slot : global_map.table) {
        if (slot.count > 0) {
            entries.push_back(&slot);
        }
    }

    auto sort_cmp = [&global_map](const HashMap::Slot* a, const HashMap::Slot* b) {
        if (a->count != b->count) {
            return a->count > b->count;
        }
        std::string_view sa(&global_map.arena[a->offset], a->length);
        std::string_view sb(&global_map.arena[b->offset], b->length);
        return sa < sb;
    };

#if HAS_EXECUTION
    std::sort(std::execution::par_unseq, entries.begin(), entries.end(), sort_cmp);
#else
    std::sort(entries.begin(), entries.end(), sort_cmp);
#endif

    FILE* out = fopen(argv[2], "w");
    if (!out) {
        perror("open output");
        return 1;
    }

    char out_buf[65536];
    setvbuf(out, out_buf, _IOFBF, sizeof(out_buf));

    for (const auto* slot : entries) {
        fprintf(out, "%u %.*s\n", slot->count, slot->length, &global_map.arena[slot->offset]);
    }

    fclose(out);
    return 0;
}