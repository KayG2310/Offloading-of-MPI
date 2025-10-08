#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

// Define the Wildcard value
#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG    -1

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

ReceiveRequest* RemoveMatchingRequest(Identifier msg_id) {
    ReceiveRequest **current_ptr = (ReceiveRequest**)&receive_queue.head;
    ReceiveRequest *prev = NULL;
    int scan_count = 0;

    while (*current_ptr != NULL) {
        scan_count++;
        ReceiveRequest *current = *current_ptr;
        if (IsMatch(current->id, msg_id)) {
            *current_ptr = current->next;
            if (current == receive_queue.tail) receive_queue.tail = prev;
            receive_queue.count--;
            total_scans_prq += scan_count;
            return current;
        }
        prev = current;
        current_ptr = &current->next;
    }
    total_scans_prq += scan_count;
    return NULL;
}

// --- Core Assignment Functions ---

void PostReceive(int rank, int tag) {
    total_operations++;
    Identifier req_id = {rank, tag};

    printf("\n--- PostReceive "); print_id(req_id); printf(" ---\n");
    printf("1. Scanning UMQ for match...\n");

    Message *matched_msg = RemoveMatchingMessage(req_id);

    if (matched_msg != NULL) {
        total_matches++;
        printf("2. ✅ Match found in UMQ. Message from Rank %d, Tag %d processed.\n", 
               matched_msg->id.source_rank, matched_msg->id.tag);
        free(matched_msg);
    } else {
        ReceiveRequest *new_req = (ReceiveRequest*)malloc(sizeof(ReceiveRequest));
        if (new_req == NULL) { perror("malloc failed"); exit(EXIT_FAILURE); }
        new_req->id = req_id;
        new_req->next = NULL;

        if (receive_queue.tail == NULL) receive_queue.head = new_req;
        else ((ReceiveRequest*)receive_queue.tail)->next = new_req;
        receive_queue.tail = new_req;
        receive_queue.count++;

        printf("2. ❌ No match in UMQ. Request appended to PRQ (Size: %d).\n", receive_queue.count);
    }
}

void MessageArrives(int rank, int tag) {
    total_operations++;
    Identifier msg_id = {rank, tag};

    printf("\n--- MessageArrives (Rank: %d, Tag: %d) ---\n", rank, tag);
    printf("1. Scanning PRQ for match...\n");

    ReceiveRequest *matched_req = RemoveMatchingRequest(msg_id);

    if (matched_req != NULL) {
        total_matches++;
        printf("2. ✅ Match found in PRQ. Message processed against posted request ");
        print_id(matched_req->id);
        printf(".\n");
        free(matched_req);
    } else {
        Message *new_msg = (Message*)malloc(sizeof(Message));
        if (new_msg == NULL) { perror("malloc failed"); exit(EXIT_FAILURE); }
        new_msg->id = msg_id;
        new_msg->next = NULL;

        if (unexpected_queue.tail == NULL) unexpected_queue.head = new_msg;
        else ((Message*)unexpected_queue.tail)->next = new_msg;
        unexpected_queue.tail = new_msg;
        unexpected_queue.count++;

        printf("2. ❌ No match in PRQ. Message stored in UMQ (Size: %d).\n", unexpected_queue.count);
    }
}

void DisplayStatistics(const char* scenario) {
    long long total_scans = total_scans_prq + total_scans_umq;
    double matching_rate = 0.0;
    
    if (total_scans > 0) {
        matching_rate = (double)total_matches / total_scans;
    }

    printf("\n======================================================\n");
    printf("📊 STATISTICS FOR SCENARIO: %s\n", scenario);
    printf("======================================================\n");
    printf("Total Operations (Post/Arrives): %lld\n", total_operations);
    printf("Total Successful Matches:        %lld\n", total_matches);
    printf("Total Queue Scans (PRQ + UMQ):   %lld (PRQ: %lld + UMQ: %lld)\n", 
           total_scans, total_scans_prq, total_scans_umq);
    printf("MESSAGE MATCHING RATE:           %.4f (Matches / Total Scans)\n", matching_rate);
    printf("======================================================\n");
}

void ResetState() {
    Message *current_msg = (Message*)unexpected_queue.head;
    while (current_msg != NULL) { Message *next = current_msg->next; free(current_msg); current_msg = next; }
    unexpected_queue.head = unexpected_queue.tail = NULL;
    unexpected_queue.count = 0;

    ReceiveRequest *current_req = (ReceiveRequest*)receive_queue.head;
    while (current_req != NULL) { ReceiveRequest *next = current_req->next; free(current_req); current_req = next; }
    receive_queue.head = receive_queue.tail = NULL;
    receive_queue.count = 0;

    total_matches = 0;
    total_scans_prq = 0;
    total_scans_umq = 0;
    total_operations = 0;
}

// --- Isolated Scenario to prevent memory corruption of the Part B Input ---

void run_partb_input_scenario() {
    
    printf("\n\n--- Running SCENARIO 3: Part B Input (High Efficiency) ---\n");

    // 1. Post all four Receives (R1, R2, R3, R4) in order.
    PostReceive(1, 10);             
    PostReceive(MPI_ANY_SOURCE, 10); 
    PostReceive(1, MPI_ANY_TAG);    
    PostReceive(MPI_ANY_SOURCE, MPI_ANY_TAG); 
    
    printf("\n>>> PRQ is fully populated with R1, R2, R3, R4 in order. <<<");

    // 2. Process all four Messages (M0, M1, M2, M3) sequentially.
    MessageArrives(1, 10);
    MessageArrives(2, 10);
    MessageArrives(1, 99);
    MessageArrives(1, 10);

    DisplayStatistics("Part B Input (Forced Sequential Matching)");
    ResetState();
}


// --- Main Execution (All Scenarios) ---

int main() {
    
    printf("--- Part A: CPU-Based MPI Message Matching (Sequential/Linear Search) ---\n");

    // =========================================================================
    // SCENARIO 1: NO CONFLICT (Optimal Ordering) - Required
    // =========================================================================
    printf("\n\n--- Running SCENARIO 1: NO CONFLICT ---\n");
    
    MessageArrives(10, 1);    
    PostReceive(10, 1);      

    PostReceive(20, 2);      
    MessageArrives(20, 2);   

    PostReceive(MPI_ANY_SOURCE, 3);
    MessageArrives(33, 3);
    
    DisplayStatistics("NO CONFLICT (Optimal Ordering)");
    ResetState();


    // =========================================================================
    // SCENARIO 2: WITH CONFLICT (Forced Long Scans) - Required
    // =========================================================================
    printf("\n\n--- Running SCENARIO 2: WITH CONFLICT ---\n");

    const int FORCED_SIZE = 10; 

    for (int i = FORCED_SIZE; i >= 1; i--) {
        PostReceive(i, i * 10); 
    }
    printf("\n>>> PRQ is now %d elements long <<<\n", FORCED_SIZE);
    
    MessageArrives(1, 10); 

    for (int i = 1; i <= FORCED_SIZE; i++) {
        MessageArrives(90 + i, 90 + i);
    }
    printf("\n>>> UMQ is now %d elements long <<<\n", FORCED_SIZE);

    PostReceive(100, 100); 

    DisplayStatistics("WITH CONFLICT (Forced Long Scans)");
    ResetState();
    
    
    // =========================================================================
    // SCENARIO 3: PART B INPUT (Demonstration of sequential processing)
    // =========================================================================
    run_partb_input_scenario();

    return 0;
}