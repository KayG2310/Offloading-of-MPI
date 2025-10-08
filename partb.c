#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#define HASH_SIZE 10
#define MAX_MSGS 10

typedef struct Receive {
    int id;
    int source;
    int tag;
    int wildcard_source;
    int wildcard_tag;
    int taken;
    struct Receive *next;
} Receive;

typedef struct Message {
    int id;
    int source;
    int tag;
} Message;

Receive* hash_no_wildcard[HASH_SIZE];
Receive* hash_source_wildcard[HASH_SIZE];
Receive* hash_tag_wildcard[HASH_SIZE];
Receive* wildcard_list = NULL;

pthread_mutex_t conflict_lock = PTHREAD_MUTEX_INITIALIZER;

int total_matches = 0;
int conflicts = 0;

int hash_func(int key) { return key % HASH_SIZE; }

void insert_hash(Receive* table[], int key, Receive* r) {
    int idx = hash_func(key);
    r->next = table[idx];
    table[idx] = r;
}

void insert_linkedlist(Receive** head, Receive* r) {
    r->next = *head;
    *head = r;
}

Receive* create_receive(int id, int src, int tag, int ws, int wt) {
    Receive* r = malloc(sizeof(Receive));
    r->id = id; r->source = src; r->tag = tag;
    r->wildcard_source = ws; r->wildcard_tag = wt;
    r->taken = 0; r->next = NULL;
    return r;
}

Receive* match_message(Message msg, int thread_id) {
    int key_src_tag = msg.source + msg.tag;
    Receive* r = NULL;

    // 1️⃣ No wildcards
    int idx = hash_func(key_src_tag);
    for (r = hash_no_wildcard[idx]; r; r = r->next) {
        if (!r->taken && r->source == msg.source && r->tag == msg.tag) {
            printf("[Thread %d] Message (Src=%d, Tag=%d) matched in HashTable-NoWildcards → Receive %d\n",
                   thread_id, msg.source, msg.tag, r->id);
            return r;
        }
    }

    // 2️⃣ Source wildcard
    idx = hash_func(msg.tag);
    for (r = hash_source_wildcard[idx]; r; r = r->next) {
        if (!r->taken && r->tag == msg.tag) {
            printf("[Thread %d] Message (Src=%d, Tag=%d) matched in HashTable-SourceWildcard → Receive %d\n",
                   thread_id, msg.source, msg.tag, r->id);
            return r;
        }
    }

    // 3️⃣ Tag wildcard
    idx = hash_func(msg.source);
    for (r = hash_tag_wildcard[idx]; r; r = r->next) {
        if (!r->taken && r->source == msg.source) {
            printf("[Thread %d] Message (Src=%d, Tag=%d) matched in HashTable-TagWildcard → Receive %d\n",
                   thread_id, msg.source, msg.tag, r->id);
            return r;
        }
    }

    // 4️⃣ Both wildcards
    for (r = wildcard_list; r; r = r->next) {
        if (!r->taken) {
            printf("[Thread %d] Message (Src=%d, Tag=%d) matched in LinkedList-BothWildcards → Receive %d\n",
                   thread_id, msg.source, msg.tag, r->id);
            return r;
        }
    }

    printf("[Thread %d] Message (Src=%d, Tag=%d) has no match\n", thread_id, msg.source, msg.tag);
    return NULL;
}

void* thread_func(void* arg) {
    Message* msg = (Message*) arg;
    Receive* r = match_message(*msg, msg->id);
    if (r) {
        pthread_mutex_lock(&conflict_lock);
        total_matches++;
        if (r->taken) {
            conflicts++;
            printf("⚠️  Conflict detected: Message %d tried to take Receive %d already taken!\n",
                   msg->id, r->id);
        }
        r->taken = 1;
        pthread_mutex_unlock(&conflict_lock);
    }
    pthread_exit(NULL);
}

int main() {
    // Step 1: Create receives
    Receive* r1 = create_receive(1, 1, 10, 0, 0);
    Receive* r2 = create_receive(2, -1, 10, 1, 0);
    Receive* r3 = create_receive(3, 1, -1, 0, 1);
    Receive* r4 = create_receive(4, -1, -1, 1, 1);

    insert_hash(hash_no_wildcard, r1->source + r1->tag, r1);
    insert_hash(hash_source_wildcard, r2->tag, r2);
    insert_hash(hash_tag_wildcard, r3->source, r3);
    insert_linkedlist(&wildcard_list, r4);

    // Step 2: Incoming messages
    Message msgs[] = {
        {0, 1, 10}, {1, 2, 10}, {2, 1, 99}, {3, 1, 10}
    };
    int num_msgs = 4;

    pthread_t threads[num_msgs];

    // Step 3: Launch all threads
    for (int i = 0; i < num_msgs; i++) {
        pthread_create(&threads[i], NULL, thread_func, &msgs[i]);
    }

    // Step 4: Wait for all to finish
    for (int i = 0; i < num_msgs; i++) {
        pthread_join(threads[i], NULL);
    }

    // Step 5: Print summary
    printf("\n===== Summary =====\n");
    printf("Total messages processed: %d\n", num_msgs);
    printf("Successful matches: %d\n", total_matches);
    printf("Conflicts detected: %d\n", conflicts);
    printf("Matching rate (No Conflict): %.2f%%\n",
           100.0 * (total_matches - conflicts) / num_msgs);
    printf("Matching rate (With Conflict): %.2f%%\n",
           100.0 * total_matches / num_msgs);

    return 0;
}
