#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h> 
#include <math.h>   

// Define the Wildcard value and constants
#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG    -1
#define MAX_EVENTS     10000 
#define LINE_BUFFER    10000

// --- Data Structures ---

typedef struct {
    int source_rank;
    int tag;
} Identifier;

typedef struct Message {
    Identifier id;
    struct Message* next; 
} Message;

typedef struct ReceiveRequest {
    Identifier id;
    struct ReceiveRequest* next;
} ReceiveRequest;

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

// --- Trace Event Structures for Input ---

typedef enum { POST, ARRIVES } EventType;

typedef struct {
    EventType type;
    int source_rank; // Requested Source (POST) or Sender (ARRIVES)
    int tag;
    double timestamp;
} TraceEvent;

TraceEvent all_events[MAX_EVENTS];
int total_events_loaded = 0;

// -----------------------------------------------------------------------------
// --- Core Matching and Helper Functions (Part A Logic) ---
// -----------------------------------------------------------------------------

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

Message* RemoveMatchingMessage(Identifier req_id) {
    Message **current_ptr = (Message**)&unexpected_queue.head;
    Message *prev = NULL;
    int scan_count = 0;

    while (*current_ptr != NULL) {
        scan_count++; Message *current = *current_ptr;
        if (IsMatch(req_id, current->id)) {
            *current_ptr = current->next;
            if (current == unexpected_queue.tail) unexpected_queue.tail = prev;
            unexpected_queue.count--;
            total_scans_umq += scan_count; return current;
        }
        prev = current; current_ptr = &current->next;
    }
    total_scans_umq += scan_count; return NULL;
}

ReceiveRequest* RemoveMatchingRequest(Identifier msg_id) {
    ReceiveRequest **current_ptr = (ReceiveRequest**)&receive_queue.head;
    ReceiveRequest *prev = NULL;
    int scan_count = 0;

    while (*current_ptr != NULL) {
        scan_count++; ReceiveRequest *current = *current_ptr;
        if (IsMatch(current->id, msg_id)) {
            *current_ptr = current->next;
            if (current == receive_queue.tail) receive_queue.tail = prev;
            receive_queue.count--;
            total_scans_prq += scan_count; return current;
        }
        prev = current; current_ptr = &current->next;
    }
    total_scans_prq += scan_count; return NULL;
}

void PostReceive(int rank, int tag) {
    total_operations++; Identifier req_id = {rank, tag};
    printf("\n--- PostReceive "); print_id(req_id); printf(" ---\n");
    Message *matched_msg = RemoveMatchingMessage(req_id);
    if (matched_msg != NULL) {
        total_matches++;
        printf("2. Match found in UMQ. Message from Rank %d, Tag %d processed.\n", matched_msg->id.source_rank, matched_msg->id.tag);
        free(matched_msg);
    } else {
        ReceiveRequest *new_req = (ReceiveRequest*)malloc(sizeof(ReceiveRequest));
        if (new_req == NULL) { perror("malloc failed"); exit(EXIT_FAILURE); }
        new_req->id = req_id; new_req->next = NULL;
        if (receive_queue.tail == NULL) receive_queue.head = new_req;
        else ((ReceiveRequest*)receive_queue.tail)->next = new_req;
        receive_queue.tail = new_req; receive_queue.count++;
        printf("2. No match in UMQ. Request appended to PRQ (Size: %d).\n", receive_queue.count);
    }
}

void MessageArrives(int rank, int tag) {
    total_operations++; Identifier msg_id = {rank, tag};
    printf("\n--- MessageArrives (Rank: %d, Tag: %d) ---\n", rank, tag);
    ReceiveRequest *matched_req = RemoveMatchingRequest(msg_id);
    if (matched_req != NULL) {
        total_matches++;
        printf("2. Match found in PRQ. Message processed against posted request "); print_id(matched_req->id); printf(".\n");
        free(matched_req);
    } else {
        Message *new_msg = (Message*)malloc(sizeof(Message));
        if (new_msg == NULL) { perror("malloc failed"); exit(EXIT_FAILURE); }
        new_msg->id = msg_id; new_msg->next = NULL;
        if (unexpected_queue.tail == NULL) unexpected_queue.head = new_msg;
        else ((Message*)unexpected_queue.tail)->next = new_msg;
        unexpected_queue.tail = new_msg; unexpected_queue.count++;
        printf("2. No match in PRQ. Message stored in UMQ (Size: %d).\n", unexpected_queue.count);
    }
}

void DisplayStatistics(const char* scenario) {
    long long total_scans = total_scans_prq + total_scans_umq;
    double matching_rate = 0.0;
    if (total_scans > 0) { matching_rate = (double)total_matches / total_scans; }
    printf("\n======================================================\n");
    printf("📊 STATISTICS FOR SCENARIO: %s\n", scenario);
    printf("Total Operations (Post/Arrives): %lld\n", total_operations);
    printf("Total Successful Matches:        %lld\n", total_matches);
    printf("Total Queue Scans (PRQ + UMQ):   %lld (PRQ: %lld + UMQ: %lld)\n", total_scans, total_scans_prq, total_scans_umq);
    printf("MESSAGE MATCHING RATE:           %.4f (Matches / Total Scans)\n", matching_rate);
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
}

// -----------------------------------------------------------------------------
// --- TXT Parsing and Trace Execution Functions ---
// -----------------------------------------------------------------------------

/**
 * @brief Parses a line from a space-separated TXT file using sscanf.
 * @return 0 on success, -1 on error.
 */
int load_txt_line(char* line, EventType type) {
    if (total_events_loaded >= MAX_EVENTS) {
        fprintf(stderr, "Error: Max events reached.\n"); return -1;
    }
    
    int rank_ignored, tag, source_rank, dest_ignored = 0; // Rank and Dest are ignored/inferred
    double timestamp;

    // Skip the header line (non-numeric start)
    if (line[0] < '0' || line[0] > '9') { return 0; }
    
    int items_read = 0;

    if (type == POST) {
        // post.txt format: rank tag source timestamp (4 items)
        // We only care about tag, source, and timestamp
        items_read = sscanf(line, "%d %d %d %lf", 
                            &rank_ignored, &tag, &source_rank, &timestamp);

        if (items_read < 4) return 0; // Skip incomplete lines
        
    } else { // ARRIVES
        // messages.txt format: rank tag source dest time (5 items)
        items_read = sscanf(line, "%d %d %d %d %lf", 
                            &rank_ignored, &tag, &source_rank, &dest_ignored, &timestamp);
        
        if (items_read < 5) return 0; // Skip incomplete lines
    }

    all_events[total_events_loaded].type = type;
    // For POST, source_rank is the requested source. For ARRIVES, it's the sender.
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
    
    // 1. Primary sort: timestamp
    if (eventA->timestamp < eventB->timestamp) return -1;
    if (eventA->timestamp > eventB->timestamp) return 1;
    
    // 2. Secondary sort: POST before ARRIVES (Tie-breaker)
    if (eventA->type == POST && eventB->type == ARRIVES) return -1;
    if (eventA->type == ARRIVES && eventB->type == POST) return 1;
    
    return 0;
}

void run_txt_trace_scenario() {
    ResetState(); 

    printf("\n\n--- RUNNING TRACE DATA SCENARIO (Time-Based Interleaving) ---\n");

    // 1. Read and load data from both TXT files
    read_events_from_txt("post.txt", POST);
    read_events_from_txt("messages.txt", ARRIVES);
    
    if (total_events_loaded == 0) {
        printf("No valid events loaded. Skipping simulation. Ensure post.txt and messages.txt exist and are formatted correctly.\n");
        return;
    }
    
    printf(">>> Loaded %d total events. Sorting by timestamp... <<<\n", total_events_loaded);

    // 2. Sort the combined array by timestamp
    qsort(all_events, total_events_loaded, sizeof(TraceEvent), compare_events);

    // 3. Process events chronologically
    for (int i = 0; i < total_events_loaded; i++) {
        TraceEvent *e = &all_events[i];
        
        printf("\n[Time: %.6f] Event %d: ", e->timestamp, i + 1);

        if (e->type == POST) {
            // Note: Rank is the requested source in the POST file.
            PostReceive(e->source_rank, e->tag);
        } else {
            // Note: Rank is the sender in the MESSAGES file.
            MessageArrives(e->source_rank, e->tag);
        }
    }
    
    // 4. Display results
    DisplayStatistics("Trace Data Time-Interleaved");
}

// -----------------------------------------------------------------------------
// --- Main Function (Entry Point) ---
// -----------------------------------------------------------------------------

int main() {
    
    printf("--- Part A: CPU-Based MPI Message Matching (Trace Simulation) ---\n");

    run_txt_trace_scenario();

    return 0;
}