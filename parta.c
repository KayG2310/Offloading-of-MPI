#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h> 
#include <math.h> 

// Define MPI Wildcard values
#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG    -1
#define MAX_EVENTS     10000 
#define LINE_BUFFER    10000

// --- Data Structures ---
typedef struct {
    int source_rank;
    int tag;
} Identifier;

typedef struct ReceiveRequest {
    Identifier id;
    double timestamp; 
    struct ReceiveRequest* next;
} ReceiveRequest;

typedef struct Message {
    Identifier id;
    double timestamp; 
    struct Message* next; 
} Message;

typedef struct {
    void* head;
    void* tail;
    int count;
} Queue;

// Global Queues and Statistics
Queue unexpected_queue = {NULL, NULL, 0}; // UMQ
Queue receive_queue = {NULL, NULL, 0};    // PRQ

long long total_matches = 0;
long long total_scans_prq = 0;
long long total_scans_umq = 0;
long long total_operations = 0;
long long total_ambiguous_conflicts = 0; // Contention Conflict Counter

// --- Trace Event Structures ---
typedef enum { POST, ARRIVES } EventType;

typedef struct {
    EventType type;
    int source_rank; 
    int tag;
    double timestamp;
} TraceEvent;

TraceEvent all_events[MAX_EVENTS];
int total_events_loaded = 0;

// -----------------------------------------------------------------------------
// --- Helper Functions ---
void print_id(Identifier id) {
    char rank_str[15], tag_str[15];
    sprintf(rank_str, "%d", id.source_rank);
    sprintf(tag_str, "%d", id.tag);
    if (id.source_rank == MPI_ANY_SOURCE) sprintf(rank_str, "ANY_SOURCE");
    if (id.tag == MPI_ANY_TAG) sprintf(tag_str, "ANY_TAG");
    printf("(Rank: %s, Tag: %s)", rank_str, tag_str);
}

bool IsMatch(Identifier req_id, Identifier msg_id) {
    bool rank_match = (req_id.source_rank == msg_id.source_rank) || 
                      (req_id.source_rank == MPI_ANY_SOURCE); 
    bool tag_match = (req_id.tag == msg_id.tag) || 
                      (req_id.tag == MPI_ANY_TAG);
    return rank_match && tag_match;
}

// -----------------------------------------------------------------------------
// --- Queue Operations ---
Message* RemoveMatchingMessage(Identifier req_id) {
    Message **current_ptr = (Message**)&unexpected_queue.head;
    Message *prev = NULL;
    int scan_count = 0;

    while (*current_ptr != NULL) {
        scan_count++; 
        Message *current = *current_ptr;
        if (IsMatch(req_id, current->id)) {
            *current_ptr = current->next;
            if (current == unexpected_queue.tail) unexpected_queue.tail = prev;
            unexpected_queue.count--;
            total_scans_umq += scan_count; 
            return current;
        }
        prev = current; 
        current_ptr = &current->next;
    }
    total_scans_umq += scan_count; 
    return NULL;
}

// Finds the oldest matching request (C1)
ReceiveRequest* FindMatchingRequest(Identifier msg_id, int *scan_count, ReceiveRequest **prev_ptr) {
    ReceiveRequest *best_match = NULL;
    ReceiveRequest *cur = (ReceiveRequest*)receive_queue.head;
    *scan_count = 0;

    while (cur != NULL) {
        (*scan_count)++;
        if (IsMatch(cur->id, msg_id)) {
            best_match = cur;
            break; // C1: First match from head is the oldest
        }
        cur = cur->next;
    }
    total_scans_prq += *scan_count;
    *prev_ptr = NULL; 
    return best_match;
}

// Removes a specific request after FindMatchingRequest finds the winner
ReceiveRequest* RemoveSpecificRequest(ReceiveRequest *to_remove) {
    ReceiveRequest **current_ptr = (ReceiveRequest**)&receive_queue.head;
    ReceiveRequest *prev = NULL;

    while (*current_ptr != NULL) {
        ReceiveRequest *current = *current_ptr;
        if (current == to_remove) {
            *current_ptr = current->next;
            if (current == receive_queue.tail) receive_queue.tail = prev;
            receive_queue.count--;
            return current;
        }
        prev = current; 
        current_ptr = &current->next;
    }
    return NULL;
}

int CountMatchingRequests(Identifier msg_id) {
    int count = 0;
    ReceiveRequest *cur = (ReceiveRequest*)receive_queue.head;
    while (cur != NULL) {
        if (IsMatch(cur->id, msg_id)) count++;
        cur = cur->next;
    }
    return count;
}

int CountMatchingMessages(Identifier req_id) {
    int count = 0;
    Message *cur = (Message*)unexpected_queue.head;
    while (cur != NULL) {
        if (IsMatch(req_id, cur->id)) count++;
        cur = cur->next;
    }
    return count;
}

// -----------------------------------------------------------------------------
// --- Event Handling ---
void PostReceive(int rank, int tag, double timestamp) {
    total_operations++; 
    Identifier req_id = {rank, tag};
    
    printf("\n[Time: %.6f] Event %lld: ", timestamp, total_operations); 
    printf("--- PostReceive "); 
    print_id(req_id); 

    // --- Ambiguity Conflict Check (Contention) ---
    int ambiguity = CountMatchingMessages(req_id);
    if (ambiguity > 1) {
        total_ambiguous_conflicts++;
        printf(" ⚠️ AMBIGUOUS CONFLICT: Receive matches %d unexpected messages.", ambiguity);
    }

    Message *found_msg = RemoveMatchingMessage(req_id); 
    
    if (found_msg != NULL) {
        total_matches++;
        // UMQ activity = Traditional Conflict
        printf(" [WITH CONFLICT] ---\n"); 
        printf("2. Match found in UMQ. Message from Rank %d, Tag %d processed. UMQ Size: %d.\n", found_msg->id.source_rank, found_msg->id.tag, unexpected_queue.count);
        free(found_msg);
    } else {
        // PRQ activity = No Traditional Conflict
        ReceiveRequest *new_req = (ReceiveRequest*)malloc(sizeof(ReceiveRequest));
        if (new_req == NULL) { perror("malloc failed"); exit(EXIT_FAILURE); }
        new_req->id = req_id; 
        new_req->timestamp = timestamp;
        new_req->next = NULL;
        
        if (receive_queue.tail == NULL) receive_queue.head = new_req;
        else ((ReceiveRequest*)receive_queue.tail)->next = new_req;
        receive_queue.tail = new_req; 
        receive_queue.count++;
        
        printf(" [NO CONFLICT] ---\n");
        printf("2. No match in UMQ. Request appended to PRQ (Size: %d).\n", receive_queue.count);
    }
}

void MessageArrives(int rank, int tag, double timestamp) {
    total_operations++; 
    Identifier msg_id = {rank, tag};
    
    printf("\n[Time: %.6f] Event %lld: ", timestamp, total_operations); 
    printf("--- MessageArrives (Rank: %d, Tag: %d) ", rank, tag);

    // --- Ambiguity Conflict Check (Contention) ---
    int ambiguity = CountMatchingRequests(msg_id);
    if (ambiguity > 1) {
        total_ambiguous_conflicts++;
        printf(" ⚠️ AMBIGUOUS CONFLICT: Message matches %d posted receives.", ambiguity);
    }

    int scan_count;
    ReceiveRequest *prev_ptr;
    ReceiveRequest *matched_req = FindMatchingRequest(msg_id, &scan_count, &prev_ptr);
    
    if (matched_req != NULL) {
        total_matches++;
        RemoveSpecificRequest(matched_req); 
        // PRQ activity = No Traditional Conflict
        printf("[NO CONFLICT] ---\n");
        printf("2. Match found in PRQ. Message processed against posted request "); 
        print_id(matched_req->id); 
        printf(".\n");
        free(matched_req);
    } else {
        // UMQ activity = Traditional Conflict
        Message *new_msg = (Message*)malloc(sizeof(Message));
        if (new_msg == NULL) { perror("malloc failed"); exit(EXIT_FAILURE); }
        new_msg->id = msg_id; 
        new_msg->timestamp = timestamp;
        new_msg->next = NULL;
        
        if (unexpected_queue.tail == NULL) unexpected_queue.head = new_msg;
        else ((Message*)unexpected_queue.tail)->next = new_msg;
        unexpected_queue.tail = new_msg; 
        unexpected_queue.count++;
        
        printf("[WITH CONFLICT] ---\n");
        printf("2. No match in PRQ. Message stored in UMQ (Size: %d).\n", unexpected_queue.count);
    }
}

// -----------------------------------------------------------------------------
// --- Statistics, Reset, File Loading, Main Functions ---
// -----------------------------------------------------------------------------

void DisplayStatistics(const char* scenario) {
    double total_time_elapsed = 0.0;
    if (total_operations > 1) {
        total_time_elapsed = all_events[total_operations - 1].timestamp - all_events[0].timestamp;
    }
    double matches_per_second = 0.0;
    if (total_time_elapsed > 0) {
        matches_per_second = (double)total_matches / total_time_elapsed;
    }
    
    long long total_scans = total_scans_prq + total_scans_umq;
    double matching_rate = 0.0;
    if (total_scans > 0) { matching_rate = (double)total_matches / total_scans; }
    
    printf("\n======================================================\n");
    printf("📊 STATISTICS FOR SCENARIO: %s\n", scenario);
    printf("Total Operations (Post/Arrives): %lld\n", total_operations);
    printf("Total Successful Matches: %lld\n", total_matches);
    printf("Total Ambiguity Conflicts (c>1): %lld\n", total_ambiguous_conflicts);
    printf("Total Queue Scans (PRQ + UMQ):  %lld (PRQ: %lld + UMQ: %lld)\n", total_scans, total_scans_prq, total_scans_umq);
    printf("Matches Per Second: %.4f (Matches / Total Time)\n", matches_per_second);
    printf("MESSAGE MATCHING RATE: %.4f (Matches / Total Scans)\n", matching_rate);
    printf("======================================================\n");
}

void ResetState() {
    Message *current_msg = (Message*)unexpected_queue.head;
    while (current_msg != NULL) { Message *next = current_msg->next; free(current_msg); current_msg = next; }
    unexpected_queue.head = unexpected_queue.tail = NULL; unexpected_queue.count = 0;
    
    ReceiveRequest *current_req = (ReceiveRequest*)receive_queue.head;
    while (current_req != NULL) { ReceiveRequest *next = current_req->next; free(current_req); current_req = next; }
    receive_queue.head = receive_queue.tail = NULL; receive_queue.count = 0;
    
    total_matches = 0; total_scans_prq = 0; total_scans_umq = 0; total_operations = 0;
    total_ambiguous_conflicts = 0;
}

int load_txt_line(char* line, EventType type) {
    if (total_events_loaded >= MAX_EVENTS) {
        fprintf(stderr, "Error: Max events reached.\n"); return -1;
    }
    
    int rank_ignored, tag, source_rank, dest_ignored = 0; 
    double timestamp;

    if (line[0] < '0' || line[0] > '9') { return 0; }
    
    int items_read = 0;

    if (type == POST) {
        items_read = sscanf(line, "%d %d %d %lf", &rank_ignored, &tag, &source_rank, &timestamp);
        if (items_read < 4) return 0;
    } else { // ARRIVES
        items_read = sscanf(line, "%d %d %d %d %lf", &rank_ignored, &tag, &source_rank, &dest_ignored, &timestamp);
        if (items_read < 5) return 0;
    }

    all_events[total_events_loaded].type = type;
    all_events[total_events_loaded].source_rank = source_rank; 
    all_events[total_events_loaded].tag = tag;
    all_events[total_events_loaded].timestamp = timestamp;
    total_events_loaded++;
    
    return 0;
}

void read_events_from_txt(const char* filename, EventType type) {
    FILE *file = fopen(filename, "r");
    if (file == NULL) {
        fprintf(stderr, "Error: Could not open file %s. Execution terminated.\n", filename);
        exit(EXIT_FAILURE);
    }
    
    char line[LINE_BUFFER];
    while (fgets(line, LINE_BUFFER, file) != NULL) {
        load_txt_line(line, type);
    }
    
    fclose(file);
}

int compare_events(const void *a, const void *b) {
    TraceEvent *eventA = (TraceEvent *)a;
    TraceEvent *eventB = (TraceEvent *)b;
    
    if (eventA->timestamp < eventB->timestamp) return -1;
    if (eventA->timestamp > eventB->timestamp) return 1;
    
    if (eventA->type == POST && eventB->type == ARRIVES) return -1;
    if (eventA->type == ARRIVES && eventB->type == POST) return 1;
    
    return 0;
}

void run_txt_trace_scenario() {
    ResetState(); 
    total_events_loaded = 0; 

    printf("--- RUNNING TRACE DATA SCENARIO (Time-Based Interleaving) ---\n");

    read_events_from_txt("post.txt", POST);
    read_events_from_txt("messages.txt", ARRIVES);
    
    if (total_events_loaded == 0) {
        printf("No valid events loaded. Skipping simulation. Ensure post.txt and messages.txt exist and are formatted correctly.\n");
        return;
    }
    
    printf(">>> Loaded %d total events. Sorting by timestamp... <<<\n", total_events_loaded);

    qsort(all_events, total_events_loaded, sizeof(TraceEvent), compare_events);

    for (int i = 0; i < total_events_loaded; i++) {
        TraceEvent *e = &all_events[i];
        
        if (e->type == POST) {
            PostReceive(e->source_rank, e->tag, e->timestamp);
        } else {
            MessageArrives(e->source_rank, e->tag, e->timestamp);
        }
    }
    
    DisplayStatistics("Trace Data Time-Interleaved");
}

int main() {
    
    printf("--- Part A: CPU-Based MPI Message Matching (Trace Simulation) ---\n");

    run_txt_trace_scenario();

    return 0;
}
