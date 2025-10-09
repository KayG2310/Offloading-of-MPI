#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>
#include <sys/time.h>

#define MAX_THREADS 4
#define MAX_RECEIVES 10000
#define MAX_MESSAGES 10000
#define HASH_SIZE 256
#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG -1
#define BLOCK_SIZE 4  // N messages processed in parallel
#define MAX_SOURCES 100  // Maximum number of different sources

// Color codes for output
#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"

// Statistics structure
typedef struct {
    int total_messages;
    int successful_matches;
    int conflicts_detected;
    int conflicts_resolved;
    int fast_path_used;
    int slow_path_used;
    int unexpected_messages;
    int matches_without_conflict;
    double total_time_with_conflicts;
    double total_time_without_conflicts;
    int messages_with_conflicts;
    int messages_without_conflicts;
    double overall_wall_time;
} Statistics;

// Message structure (simplified - no rank or dest)
typedef struct {
    int tag;
    int source;
    double timestamp;
    int message_id;
} Message;

// Receive descriptor (simplified - no rank)
typedef struct Receive {
    int tag;
    int source;
    double timestamp;
    int label;  // Monotonically increasing counter
    int sequence_id;
    uint32_t booking_bitmap;
    bool consumed;
    struct Receive* next;
    struct Receive* prev;
    pthread_mutex_t recv_lock;  // Per-receive lock for atomic operations
} Receive;

// Hash bucket
typedef struct {
    Receive* head;
    Receive* tail;
    pthread_mutex_t lock;
} HashBucket;

// Index structures for a single source
typedef struct {
    HashBucket no_wildcard[HASH_SIZE];      // Hash(tag) for specific source
    HashBucket tag_wildcard;                 // Linked list for tag wildcards
} SourceIndexes;

// Global index structures
typedef struct {
    SourceIndexes* source_indexes[MAX_SOURCES];  // Per-source indexes
    HashBucket source_wildcard[HASH_SIZE];       // Hash(tag) for source wildcards
    HashBucket both_wildcard;                    // Linked list for both wildcards
    int source_count;
    int source_map[MAX_SOURCES];                 // Maps actual source ID to index
} GlobalIndexes;

// Thread data
typedef struct {
    int thread_id;
    Message* message;
    Receive* matched_receive;
    bool has_conflict;
    bool conflict_resolved;
    Receive* candidates[4];     // Store pointers to candidate receives
    struct timeval start_time;
    struct timeval end_time;
} ThreadData;

// Global structures
GlobalIndexes indexes;
Statistics stats = {0};
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_barrier_t phase_barrier;
int global_label_counter = 0;
int global_sequence_counter = 0;
Message messages[MAX_MESSAGES];
Receive receives[MAX_RECEIVES];
int num_messages = 0;
int num_receives = 0;
ThreadData* global_thread_data[MAX_THREADS];

// Unexpected Message Queue (UMQ)
typedef struct UMQNode {
    Message* message;
    struct UMQNode* next;
    struct UMQNode* prev;
    bool consumed;
} UMQNode;

typedef struct {
    UMQNode* head;
    UMQNode* tail;
    pthread_mutex_t lock;
    int size;
} UMQ;

UMQ umq = { .head = NULL, .tail = NULL, .lock = PTHREAD_MUTEX_INITIALIZER, .size = 0 };

// Ordered conflict resolution synchronization (per block)
pthread_mutex_t conflict_order_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t conflict_order_cv = PTHREAD_COND_INITIALIZER;
int conflict_committed_upto = -1; // highest thread id that has committed selection in current block

// Enqueue unmatched message into UMQ
void umq_enqueue(Message* msg) {
    UMQNode* node = (UMQNode*)malloc(sizeof(UMQNode));
    if (!node) return;
    node->message = msg;
    node->next = NULL;
    node->prev = NULL;
    node->consumed = false;
    pthread_mutex_lock(&umq.lock);
    if (umq.tail == NULL) {
        umq.head = umq.tail = node;
    } else {
        node->prev = umq.tail;
        umq.tail->next = node;
        umq.tail = node;
    }
    umq.size++;
    pthread_mutex_unlock(&umq.lock);
}

// Try to match a receive with a message in UMQ. If matched, consume and return the message pointer, else NULL
Message* umq_try_match_receive(Receive* recv) {
    pthread_mutex_lock(&umq.lock);
    UMQNode* cur = umq.head;
    while (cur) {
        if (!cur->consumed) {
            bool source_match = (recv->source == MPI_ANY_SOURCE || recv->source == cur->message->source);
            bool tag_match = (recv->tag == MPI_ANY_TAG || recv->tag == cur->message->tag);
            if (source_match && tag_match) {
                cur->consumed = true;
                // remove from list
                UMQNode* prev = cur->prev;
                UMQNode* next = cur->next;
                if (prev) prev->next = next; else umq.head = next;
                if (next) next->prev = prev; else umq.tail = prev;
                umq.size--;
                Message* matched = cur->message;
                free(cur);
                pthread_mutex_unlock(&umq.lock);
                return matched;
            }
        }
        cur = cur->next;
    }
    pthread_mutex_unlock(&umq.lock);
    return NULL;
}

// Hash functions
unsigned int hash_one_key(int key) {
    return (unsigned int)key % HASH_SIZE;
}

// Get or create source index
int get_source_index(int source) {
    for (int i = 0; i < indexes.source_count; i++) {
        if (indexes.source_map[i] == source) {
            return i;
        }
    }
    
    // Create new source index if not found
    if (indexes.source_count < MAX_SOURCES) {
        int idx = indexes.source_count++;
        indexes.source_map[idx] = source;
        
        // Initialize source-specific indexes
        indexes.source_indexes[idx] = malloc(sizeof(SourceIndexes));
        for (int i = 0; i < HASH_SIZE; i++) {
            indexes.source_indexes[idx]->no_wildcard[i].head = NULL;
            indexes.source_indexes[idx]->no_wildcard[i].tail = NULL;
            pthread_mutex_init(&indexes.source_indexes[idx]->no_wildcard[i].lock, NULL);
        }
        indexes.source_indexes[idx]->tag_wildcard.head = NULL;
        indexes.source_indexes[idx]->tag_wildcard.tail = NULL;
        pthread_mutex_init(&indexes.source_indexes[idx]->tag_wildcard.lock, NULL);
        
        return idx;
    }
    return -1;
}

// Initialize indexes
void init_indexes() {
    indexes.source_count = 0;
    
    // Initialize source wildcard hash table
    for (int i = 0; i < HASH_SIZE; i++) {
        indexes.source_wildcard[i].head = NULL;
        indexes.source_wildcard[i].tail = NULL;
        pthread_mutex_init(&indexes.source_wildcard[i].lock, NULL);
    }
    
    // Initialize both wildcards linked list
    indexes.both_wildcard.head = NULL;
    indexes.both_wildcard.tail = NULL;
    pthread_mutex_init(&indexes.both_wildcard.lock, NULL);
}

// Add receive to appropriate index
void add_receive_to_index(Receive* recv) {
    recv->label = global_label_counter++;
    recv->booking_bitmap = 0;
    recv->consumed = false;
    pthread_mutex_init(&recv->recv_lock, NULL);
    
    // First, try to match this receive with any unexpected message in UMQ
    Message* matched_msg = umq_try_match_receive(recv);
    if (matched_msg) {
        pthread_mutex_lock(&recv->recv_lock);
        recv->consumed = true;
        pthread_mutex_unlock(&recv->recv_lock);
        pthread_mutex_lock(&stats_lock);
        stats.successful_matches++;
        stats.matches_without_conflict++;
        pthread_mutex_unlock(&stats_lock);
        printf(GREEN "  → Matched UMQ message (source=%d, tag=%d) with receive (source=%d, tag=%d, label=%d); not indexing\n" RESET,
               matched_msg->source, matched_msg->tag, recv->source, recv->tag, recv->label);
        return; // Do not index; this receive is consumed by UMQ match
    }
    
    HashBucket* bucket = NULL;
    char* index_type = "";
    
    if (recv->source != MPI_ANY_SOURCE && recv->tag != MPI_ANY_TAG) {
        // No wildcards - store in source-specific hash table
        int src_idx = get_source_index(recv->source);
        if (src_idx >= 0) {
            unsigned int hash = hash_one_key(recv->tag);
            bucket = &indexes.source_indexes[src_idx]->no_wildcard[hash];
            index_type = "Source-Specific No Wildcard Hash Table";
            printf(GREEN "  → Added to %s for Source %d [Hash=%u] (tag=%d)\n" RESET, 
                   index_type, recv->source, hash, recv->tag);
        }
    } else if (recv->source == MPI_ANY_SOURCE && recv->tag != MPI_ANY_TAG) {
        // Source wildcard
        unsigned int hash = hash_one_key(recv->tag);
        bucket = &indexes.source_wildcard[hash];
        index_type = "Source Wildcard Hash Table";
        printf(YELLOW "  → Added to %s [Hash=%u] (tag=%d)\n" RESET, 
               index_type, hash, recv->tag);
    } else if (recv->source != MPI_ANY_SOURCE && recv->tag == MPI_ANY_TAG) {
        // Tag wildcard - store in source-specific linked list
        int src_idx = get_source_index(recv->source);
        if (src_idx >= 0) {
            bucket = &indexes.source_indexes[src_idx]->tag_wildcard;
            index_type = "Source-Specific Tag Wildcard Linked List";
            printf(CYAN "  → Added to %s for Source %d\n" RESET, 
                   index_type, recv->source);
        }
    } else {
        // Both wildcards
        bucket = &indexes.both_wildcard;
        index_type = "Both Wildcards Linked List";
        printf(MAGENTA "  → Added to %s\n" RESET, index_type);
    }
    
    if (bucket) {
        // Add to linked list in bucket
        pthread_mutex_lock(&bucket->lock);
        if (bucket->tail == NULL) {
            bucket->head = bucket->tail = recv;
            recv->prev = recv->next = NULL;
        } else {
            recv->prev = bucket->tail;
            recv->next = NULL;
            bucket->tail->next = recv;
            bucket->tail = recv;
        }
        pthread_mutex_unlock(&bucket->lock);
    }
}

// Search for matching receive in a bucket
Receive* search_bucket(HashBucket* bucket, int source, int tag, char* bucket_name) {
    pthread_mutex_lock(&bucket->lock);
    Receive* current = bucket->head;
    Receive* result = NULL;
    
    while (current) {
        if (!current->consumed) {
            bool source_match = (current->source == MPI_ANY_SOURCE || 
                                current->source == source);
            bool tag_match = (current->tag == MPI_ANY_TAG || 
                             current->tag == tag);
            if (source_match && tag_match) {
                result = current;
                printf("    Found match in %s (label=%d)\n", bucket_name, current->label);
                break;
            }
        }
        current = current->next;
    }
    pthread_mutex_unlock(&bucket->lock);
    return result;
}

// Optimistic matching phase
void optimistic_matching(ThreadData* tdata) {
    Message* msg = tdata->message;
    printf(BLUE "\n[Thread %d] Starting optimistic matching for Message (source=%d, tag=%d)\n" RESET,
           tdata->thread_id, msg->source, msg->tag);
    
    // Initialize candidates
    for (int i = 0; i < 4; i++) {
        tdata->candidates[i] = NULL;
    }
    
    // Search in source-specific no wildcard index
    int src_idx = -1;
    for (int i = 0; i < indexes.source_count; i++) {
        if (indexes.source_map[i] == msg->source) {
            src_idx = i;
            break;
        }
    }
    
    if (src_idx >= 0) {
        // Search in source-specific no wildcard hash table
        unsigned int hash = hash_one_key(msg->tag);
        char bucket_name[100];
        sprintf(bucket_name, "Source %d No-Wildcard Hash[%u]", msg->source, hash);
        Receive* recv = search_bucket(&indexes.source_indexes[src_idx]->no_wildcard[hash], 
                                     msg->source, msg->tag, bucket_name);
        if (recv) {
            tdata->candidates[0] = recv;
        }
        
        // Search in source-specific tag wildcard linked list
        sprintf(bucket_name, "Source %d Tag-Wildcard LinkedList", msg->source);
        recv = search_bucket(&indexes.source_indexes[src_idx]->tag_wildcard, 
                            msg->source, msg->tag, bucket_name);
        if (recv) {
            tdata->candidates[2] = recv;
        }
    }
    
    // Search in source wildcard hash table
    unsigned int hash = hash_one_key(msg->tag);
    char bucket_name[100];
    sprintf(bucket_name, "Source-Wildcard Hash[%u]", hash);
    Receive* recv = search_bucket(&indexes.source_wildcard[hash], 
                                 msg->source, msg->tag, bucket_name);
    if (recv) {
        tdata->candidates[1] = recv;
    }
    
    // Search in both wildcards linked list
    recv = search_bucket(&indexes.both_wildcard, msg->source, msg->tag, 
                        "Both-Wildcards LinkedList");
    if (recv) {
        tdata->candidates[3] = recv;
    }
    
    // Select receive with minimum label (oldest)
    int min_label = INT_MAX;
    tdata->matched_receive = NULL;
    
    for (int i = 0; i < 4; i++) {
        if (tdata->candidates[i] && tdata->candidates[i]->label < min_label) {
            min_label = tdata->candidates[i]->label;
            tdata->matched_receive = tdata->candidates[i];
        }
    }
    
    // Book the receive if found (atomically)
    if (tdata->matched_receive) {
        pthread_mutex_lock(&tdata->matched_receive->recv_lock);
        uint32_t bit = 1 << tdata->thread_id;
        tdata->matched_receive->booking_bitmap |= bit;
        pthread_mutex_unlock(&tdata->matched_receive->recv_lock);
        printf(GREEN "  → Selected receive with label %d\n" RESET, tdata->matched_receive->label);
    } else {
        printf(RED "  → No matching receive found (unexpected message)\n" RESET);
    }
}

// Check for conflicts based on C1 and C2 constraints
bool check_conflicts(ThreadData* tdata, int num_threads) {
    if (!tdata->matched_receive) return false;
    
    // Check booking bitmap for conflicts
    pthread_mutex_lock(&tdata->matched_receive->recv_lock);
    uint32_t bitmap = tdata->matched_receive->booking_bitmap;
    pthread_mutex_unlock(&tdata->matched_receive->recv_lock);
    
    // Check if any thread with lower ID booked the same receive
    for (int i = 0; i < tdata->thread_id; i++) {
        if (bitmap & (1 << i)) {
            printf(RED "[Thread %d] Conflict detected with Thread %d on receive label %d (C2 constraint)\n" RESET,
                   tdata->thread_id, i, tdata->matched_receive->label);
            pthread_mutex_lock(&stats_lock);
            stats.conflicts_detected++;
            pthread_mutex_unlock(&stats_lock);
            return true;
        }
    }
    return false;
}

// Fast path conflict resolution
bool try_fast_path(ThreadData* tdata, int num_threads) {
    if (!tdata->matched_receive) return false;
    Receive* original = tdata->matched_receive;
    
    // Check if all threads selected the same receive (sequence of compatible receives)
    pthread_mutex_lock(&original->recv_lock);
    uint32_t bitmap = original->booking_bitmap;
    pthread_mutex_unlock(&original->recv_lock);
    
    int booking_count = 0;
    for (int i = 0; i < num_threads; i++) {
        if (bitmap & (1 << i)) booking_count++;
    }
    
    // Fast path only if all threads selected the same receive
    if (booking_count != num_threads) {
        return false;
    }
    
    // Find the next receive in the same sequence
    Receive* current = original;
    for (int i = 0; i < tdata->thread_id; i++) {
        current = current->next;
        if (!current || current->sequence_id != original->sequence_id || current->consumed) {
            return false;
        }
    }
    
    // Check if this receive is still in the same sequence
    if (current && current->sequence_id == original->sequence_id && !current->consumed) {
        tdata->matched_receive = current;
        
        // Book the new receive
        pthread_mutex_lock(&current->recv_lock);
        uint32_t bit = 1 << tdata->thread_id;
        current->booking_bitmap |= bit;
        pthread_mutex_unlock(&current->recv_lock);
        
        pthread_mutex_lock(&stats_lock);
        stats.fast_path_used++;
        pthread_mutex_unlock(&stats_lock);
        
        printf(GREEN "[Thread %d] Fast path resolution successful (new label=%d)\n" RESET, 
               tdata->thread_id, current->label);
        return true;
    }
    return false;
}

// Slow path conflict resolution
void slow_path_resolution(ThreadData* tdata, int num_threads) {
    printf(YELLOW "[Thread %d] Using slow path resolution\n" RESET, tdata->thread_id);
    
    pthread_mutex_lock(&stats_lock);
    stats.slow_path_used++;
    pthread_mutex_unlock(&stats_lock);
    
    // Re-search for a new receive
    tdata->matched_receive = NULL;
    
    // Try candidates in order of label (C1 constraint)
    int min_label = INT_MAX;
    Receive* best_candidate = NULL;
    
    for (int i = 0; i < 4; i++) {
        if (tdata->candidates[i] && !tdata->candidates[i]->consumed && 
            tdata->candidates[i]->label < min_label) {
            
            // Check if not already taken by lower ID threads
            bool taken = false;
            for (int j = 0; j < tdata->thread_id; j++) {
                if (global_thread_data[j]->matched_receive == tdata->candidates[i]) {
                    taken = true;
                    break;
                }
            }
            
            if (!taken) {
                min_label = tdata->candidates[i]->label;
                best_candidate = tdata->candidates[i];
            }
        }
    }
    
    if (best_candidate) {
        tdata->matched_receive = best_candidate;
        printf(YELLOW "[Thread %d] Found alternative receive (label=%d)\n" RESET,
               tdata->thread_id, tdata->matched_receive->label);
    }
}

// Thread function for processing messages
void* process_message(void* arg) {
    ThreadData* tdata = (ThreadData*)arg;
    int block_threads = (num_messages < BLOCK_SIZE) ? num_messages : BLOCK_SIZE;
    
    // Store reference for inter-thread communication
    global_thread_data[tdata->thread_id] = tdata;
    
    // Record start time
    gettimeofday(&tdata->start_time, NULL);
    
    // Phase 1: Optimistic matching
    optimistic_matching(tdata);
    
    // Phase 2: Barrier - wait for all threads to complete matching
    pthread_barrier_wait(&phase_barrier);
    
    // Phase 3: Conflict detection
    tdata->has_conflict = check_conflicts(tdata, block_threads);
    
    // Phase 4: Barrier before conflict resolution
    pthread_barrier_wait(&phase_barrier);
    
    // Phase 5: Conflict resolution
    if (tdata->has_conflict) {
        if (!try_fast_path(tdata, block_threads)) {
            slow_path_resolution(tdata, block_threads);
        }
        
        pthread_mutex_lock(&stats_lock);
        stats.conflicts_resolved++;
        pthread_mutex_unlock(&stats_lock);
        
        tdata->conflict_resolved = true;
    }
    
    // Phase 6: Ordered resolution and commit per C1/C2
    pthread_mutex_lock(&conflict_order_mutex);
    // Wait until previous thread id has committed
    while (tdata->thread_id != conflict_committed_upto + 1) {
        pthread_cond_wait(&conflict_order_cv, &conflict_order_mutex);
    }
    // In commit slot: if conflicted, reselect ensuring no violation of C1/C2
    if (tdata->has_conflict) {
        // Try fast path first
        if (!try_fast_path(tdata, block_threads)) {
            slow_path_resolution(tdata, block_threads);
        }
        pthread_mutex_lock(&stats_lock);
        stats.conflicts_resolved++;
        pthread_mutex_unlock(&stats_lock);
        tdata->conflict_resolved = true;
    }
    // Commit: advance the ordered token and signal next
    conflict_committed_upto = tdata->thread_id;
    pthread_cond_broadcast(&conflict_order_cv);
    pthread_mutex_unlock(&conflict_order_mutex);

    // Phase 7: Execute protocol or handle unexpected
    if (tdata->matched_receive && !tdata->matched_receive->consumed) {
        pthread_mutex_lock(&tdata->matched_receive->recv_lock);
        if (!tdata->matched_receive->consumed) {  // Double-check
            tdata->matched_receive->consumed = true;
            
            pthread_mutex_lock(&stats_lock);
            stats.successful_matches++;
            if (!tdata->has_conflict) {
                stats.matches_without_conflict++;
            }
            pthread_mutex_unlock(&stats_lock);
            
            printf(GREEN "[Thread %d] Successfully matched message (source=%d, tag=%d) "
                   "with receive (source=%d, tag=%d, label=%d)%s\n" RESET,
                   tdata->thread_id, tdata->message->source, tdata->message->tag,
                   tdata->matched_receive->source, tdata->matched_receive->tag,
                   tdata->matched_receive->label,
                   tdata->has_conflict ? " [after conflict]" : "");
        }
        pthread_mutex_unlock(&tdata->matched_receive->recv_lock);
    } else if (!tdata->matched_receive) {
        // No matching receive yet: enqueue into UMQ for later matching
        umq_enqueue(tdata->message);
        printf(YELLOW "[Thread %d] No receive yet; enqueued message to UMQ (source=%d, tag=%d)\n" RESET,
               tdata->thread_id, tdata->message->source, tdata->message->tag);
    }
    
    // Record end time
    gettimeofday(&tdata->end_time, NULL);
    
    // Calculate time and update stats
    double elapsed = (tdata->end_time.tv_sec - tdata->start_time.tv_sec) + 
                    (tdata->end_time.tv_usec - tdata->start_time.tv_usec) / 1000000.0;
    
    pthread_mutex_lock(&stats_lock);
    if (tdata->has_conflict) {
        stats.total_time_with_conflicts += elapsed;
        stats.messages_with_conflicts++;
    } else {
        stats.total_time_without_conflicts += elapsed;
        stats.messages_without_conflicts++;
    }
    pthread_mutex_unlock(&stats_lock);
    
    return NULL;
}

// Read messages from file (without rank and dest)
int read_messages(const char* filename) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        perror("Error opening messages file");
        return -1;
    }
    
    char line[256];
    fgets(line, sizeof(line), fp);  // Skip header
    
    while (fgets(line, sizeof(line), fp) && num_messages < MAX_MESSAGES) {
        Message* msg = &messages[num_messages];
        int rank, dest;  // Read but ignore
        if (sscanf(line, "%d %d %d %d %lf", 
                   &rank, &msg->tag, &msg->source, 
                   &dest, &msg->timestamp) == 5) {
            msg->message_id = num_messages;
            num_messages++;
        }
    }
    
    fclose(fp);
    return num_messages;
}

// Read posted receives from file (without rank)
int read_receives(const char* filename) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        perror("Error opening receives file");
        return -1;
    }
    
    char line[256];
    fgets(line, sizeof(line), fp);  // Skip header
    
    while (fgets(line, sizeof(line), fp) && num_receives < MAX_RECEIVES) {
        Receive* recv = &receives[num_receives];
        int rank;  // Read but ignore
        if (sscanf(line, "%d %d %d %lf", 
                   &rank, &recv->tag, &recv->source, 
                   &recv->timestamp) == 4) {
            
            // Check for wildcards (represented as -1 in the file)
            if (recv->source == -1) recv->source = MPI_ANY_SOURCE;
            if (recv->tag == -1) recv->tag = MPI_ANY_TAG;
            
            // Set sequence ID (same sequence if same source and tag)
            if (num_receives > 0) {
                Receive* prev = &receives[num_receives - 1];
                if (prev->source == recv->source && prev->tag == recv->tag) {
                    recv->sequence_id = prev->sequence_id;
                } else {
                    recv->sequence_id = ++global_sequence_counter;
                }
            } else {
                recv->sequence_id = 0;
            }
            
            num_receives++;
        }
    }
    
    fclose(fp);
    return num_receives;
}

double now_sec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Process messages in blocks
void process_message_block(int start_idx, int block_size) {
    pthread_t threads[MAX_THREADS];
    ThreadData thread_data[MAX_THREADS];
    
    int actual_block_size = (start_idx + block_size > num_messages) ? 
                           (num_messages - start_idx) : block_size;
    
    if (actual_block_size > MAX_THREADS) {
        actual_block_size = MAX_THREADS;
    }

    double block_start = now_sec();
    // reset ordered resolution state for this block
    pthread_mutex_lock(&conflict_order_mutex);
    conflict_committed_upto = -1;
    pthread_mutex_unlock(&conflict_order_mutex);
    
    printf("\n" CYAN "════════════════════════════════════════════════════════\n" RESET);
    printf(CYAN "Processing Message Block [%d-%d]\n" RESET, 
           start_idx, start_idx + actual_block_size - 1);
    printf(CYAN "════════════════════════════════════════════════════════\n" RESET);
    
    // Reinitialize barrier for this block
    pthread_barrier_destroy(&phase_barrier);
    pthread_barrier_init(&phase_barrier, NULL, actual_block_size);
    
    // Create threads for parallel processing
    for (int i = 0; i < actual_block_size; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].message = &messages[start_idx + i];
        thread_data[i].matched_receive = NULL;
        thread_data[i].has_conflict = false;
        thread_data[i].conflict_resolved = false;
        
        pthread_create(&threads[i], NULL, process_message, &thread_data[i]);
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < actual_block_size; i++) {
        pthread_join(threads[i], NULL);
    }
    double block_end = now_sec();    // wall-clock end
    double block_elapsed = block_end - block_start;

    pthread_mutex_lock(&stats_lock);
    stats.total_messages += actual_block_size;
    stats.overall_wall_time += block_elapsed;
    pthread_mutex_unlock(&stats_lock);
}

// Print statistics with message matching rate
void print_statistics() {
    printf("\n" CYAN "════════════════════════════════════════════════════════\n" RESET);
    printf(CYAN "                    FINAL STATISTICS                      \n" RESET);
    printf(CYAN "════════════════════════════════════════════════════════\n" RESET);
    
    printf("Total Messages Processed:     %d\n", stats.total_messages);
    printf("Successful Matches:           %d\n", stats.successful_matches);
    printf("Unexpected Messages:          %d\n", stats.unexpected_messages);
    printf("Conflicts Detected:           %d\n", stats.conflicts_detected);
    printf("Conflicts Resolved:           %d\n", stats.conflicts_resolved);
    printf("Fast Path Used:              %d\n", stats.fast_path_used);
    printf("Slow Path Used:              %d\n", stats.slow_path_used);
    printf("Matches Without Conflict:    %d\n", stats.matches_without_conflict);
    
    printf("\n" GREEN "Performance Metrics:\n" RESET);
    
    // Calculate message matching rates
    double rate_with_conflicts = 0;
    double rate_without_conflicts = 0;
    
    if (stats.total_time_with_conflicts > 0) {
        rate_with_conflicts = stats.messages_with_conflicts / stats.total_time_with_conflicts;
    }
    
    if (stats.total_time_without_conflicts > 0) {
        rate_without_conflicts = stats.messages_without_conflicts / stats.total_time_without_conflicts;
    }
    
    printf("  Messages with conflicts:     %d\n", stats.messages_with_conflicts);
    printf("  Messages without conflicts:  %d\n", stats.messages_without_conflicts);
    printf("\n");
    printf("  " YELLOW "Message Matching Rate (With Conflicts):    %.2f messages/second\n" RESET, 
           rate_with_conflicts);
    printf("  " GREEN "Message Matching Rate (No Conflicts):      %.2f messages/second\n" RESET, 
           rate_without_conflicts);
    
    if (stats.total_messages > 0) {
        double overall_time = stats.total_time_with_conflicts + stats.total_time_without_conflicts;
        double overall_rate = stats.total_messages / stats.overall_wall_time;
        printf("  " CYAN "Overall Message Matching Rate:             %.2f messages/second\n" RESET, 
               overall_rate);
    }
    
    if (stats.conflicts_detected > 0) {
        double fast_path_success = (double)stats.fast_path_used / stats.conflicts_detected * 100;
        printf("\n  Fast Path Success Rate:   %.2f%%\n", fast_path_success);
    }
    
    printf(CYAN "════════════════════════════════════════════════════════\n" RESET);
}

int main() {
    printf(CYAN "╔═══════════════════════════════════════════════════════╗\n" RESET);
    printf(CYAN "║         OPTIMISTIC TAG MATCHING IMPLEMENTATION        ║\n" RESET);
    printf(CYAN "╚═══════════════════════════════════════════════════════╝\n" RESET);
    
    // Initialize
    init_indexes();
    pthread_barrier_init(&phase_barrier, NULL, MAX_THREADS);
    
    // Read input files
    printf("\n" YELLOW "Reading input files...\n" RESET);
    
    if (read_receives("post.txt") < 0) {
        fprintf(stderr, "Failed to read receives\n");
        return 1;
    }
    printf(GREEN "  ✓ Loaded %d receives from post.txt\n" RESET, num_receives);
    
    if (read_messages("messages.txt") < 0) {
        fprintf(stderr, "Failed to read messages\n");
        return 1;
    }
    printf(GREEN "  ✓ Loaded %d messages from messages.txt\n" RESET, num_messages);
    
    // Index all receives
    printf("\n" YELLOW "Indexing receives...\n" RESET);
    for (int i = 0; i < num_receives; i++) {
        printf("\nReceive %d (tag=%d, source=%d):\n", 
               i, receives[i].tag, receives[i].source);
        add_receive_to_index(&receives[i]);
    }
    
    printf("\n" YELLOW "Created %d source-specific index structures\n" RESET, indexes.source_count);
    
    // Process messages in blocks
    printf("\n" YELLOW "Processing messages in blocks of %d...\n" RESET, BLOCK_SIZE);
    for (int i = 0; i < num_messages; i += BLOCK_SIZE) {
        process_message_block(i, BLOCK_SIZE);
    }
    
    // After processing, any messages left in UMQ are unexpected
    pthread_mutex_lock(&umq.lock);
    int leftover = umq.size;
    // Free UMQ nodes
    UMQNode* cur = umq.head;
    while (cur) {
        UMQNode* next = cur->next;
        free(cur);
        cur = next;
    }
    umq.head = umq.tail = NULL;
    umq.size = 0;
    pthread_mutex_unlock(&umq.lock);
    if (leftover > 0) {
        pthread_mutex_lock(&stats_lock);
        stats.unexpected_messages += leftover;
        pthread_mutex_unlock(&stats_lock);
        printf(RED "\nUMQ leftover messages counted as unexpected: %d\n" RESET, leftover);
    }

    // Print final statistics
    print_statistics();
    
    // Cleanup
    pthread_barrier_destroy(&phase_barrier);
    for (int i = 0; i < indexes.source_count; i++) {
        free(indexes.source_indexes[i]);
    }
    
    return 0;
}