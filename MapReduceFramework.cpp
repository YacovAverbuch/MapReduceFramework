
#include "MapReduceFramework.h"
#include "Barrier.h"


#include <pthread.h>
#include <set>
#include <cstdio>
#include <atomic>
#include <algorithm>
#include <iostream>

#include <unistd.h>
#include <semaphore.h>

/**
 * trace the state of the job
 */
struct JobStateKeyTracking{
    JobStateKeyTracking(int i, int j, stage_t jobStage) : key_to_process(i), atomic_key_processed(j), jobStage(jobStage) {}
    int key_to_process;
    std::atomic<int> atomic_key_processed;
    stage_t jobStage;
};

/**
 * declaration ahead
 */
struct ThreadContext;

/**
 * the manager
 */
class JobManager{
public:

    JobManager(const MapReduceClient& client, const InputVec & inputVec, OutputVec & outputVec, int multiThreadLevel) :
            client(client), inputVec(inputVec), OutputVector(outputVec), numOfTreads(multiThreadLevel),
            jobState((int)inputVec.size(), 0, MAP_STAGE),
            get_data_mutex(PTHREAD_MUTEX_INITIALIZER),
            vector_insertion_mutex(PTHREAD_MUTEX_INITIALIZER)
    {
        if (sem_init(& semaphore, 0, 0) != 0){
            fprintf(stderr, "[[semaphore]] error on init");
            exit(1);
        }

        barrier = new Barrier(multiThreadLevel);
    }

    ~JobManager(){


        if (pthread_mutex_destroy(&get_data_mutex) != 0) {
            fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
            exit(1);
        }
        if (pthread_mutex_destroy(&vector_insertion_mutex) != 0) {
            fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
            exit(1);
        }
        if (sem_destroy(& semaphore) != 0){
            fprintf(stderr, "[[semaphore]] error on destroy");
            exit(1);
        }
        delete barrier;
    }


    const MapReduceClient& client;
    const InputVec & inputVec;
    OutputVec & OutputVector;
    int numOfTreads;

    std::vector<ThreadContext *> allThreads;
    JobStateKeyTracking jobState;

    Barrier * barrier;
    sem_t semaphore;
    pthread_mutex_t get_data_mutex;
    pthread_mutex_t vector_insertion_mutex;


    std::vector<IntermediateVec *> vectors_to_reduce;
    bool dune_shuffle_phase = false;
    bool had_join = false;
};


/**
 * lock
 * @param mutex
 */
void lock(pthread_mutex_t * mutex){
    if (pthread_mutex_lock(mutex) != 0){
        fprintf(stderr, "[[MapReduceFramework]] error on pthread_mutex_lock");
        exit(1);
    }
}

/**
 * unlock
 * @param mutex
 */
void unlock(pthread_mutex_t * mutex){
    if (pthread_mutex_unlock(mutex) != 0){
        fprintf(stderr, "[[MapReduceFramework]] error on pthread_mutex_lock");
        exit(1);
    }
}


/**
 * hold the data regarding state
 */
struct ThreadContext {
    ThreadContext(JobManager * man, int i) : manager(man), intermediateVec((unsigned long)i) {}
    JobManager * manager;
    IntermediateVec intermediateVec;
    pthread_t pthread_identifier;         // the number the OS gave to the threads
};


/**
 * comparator to sort the Intermediate vector
 * @param i
 * @param j
 * @return
 */
bool intermediateComparator (IntermediatePair i, IntermediatePair j) { return (* i.first) < (* j.first) ; }


/**
 * while there is keys, the thread that reach here take a number in a atomik way and send it to the map function
 * of the client
 * @param threadContext the thread
 */
void mapProcess(ThreadContext * threadContext){
    while((size_t)threadContext->manager->jobState.atomic_key_processed < threadContext->manager->inputVec.size())  {
        auto old_value = (size_t)threadContext->manager->jobState.atomic_key_processed ++;

        // relevant if tow threads got inside the while when there is only one key left
        if (old_value >= threadContext->manager->inputVec.size()){
            break;
        }
        const InputPair & pair = threadContext->manager->inputVec[old_value];
        threadContext->manager->client.map(pair.first, pair.second, threadContext);
    }
    std::sort(threadContext->intermediateVec.begin(), threadContext->intermediateVec.end(), intermediateComparator);
}


/**
 * while there is something to process, it wait in the semaphore and when it released, it take (in protection of mutex)
 * a vector to reduce
 * @param manager
 */
void reduceProcess(JobManager * manager){

    while( (! manager->dune_shuffle_phase) or (! manager->vectors_to_reduce.empty() ) ) {

        if (sem_wait(& manager->semaphore) != 0){
            fprintf(stderr, "[[reducer]] error on sem_wait");
            exit(1);
        }
        lock(& manager->vector_insertion_mutex);

        if(manager->vectors_to_reduce.empty()){
            unlock(& manager->vector_insertion_mutex);
            continue;
        }

        IntermediateVec * cur_intermediate_Vec = manager->vectors_to_reduce.back();
        manager->vectors_to_reduce.pop_back();

        unlock(& manager->vector_insertion_mutex);

        unsigned long size = cur_intermediate_Vec->size();
        manager->client.reduce(cur_intermediate_Vec, manager);
        manager->jobState.atomic_key_processed += size;
        delete cur_intermediate_Vec;
    }

    // at least one thread will reach here, so it will release the others (each releases the next)
    // after shuffler set state to "dune_shuffle_phase" and get here, if the queue is empty, then it get out,
    // otherwise, the semaphore is positive with the number of the vec in the queue, so the last one to take the last
    // semaphore, with the last vector, will get out and releases the others

    if (sem_post(& manager->semaphore) != 0) {
        fprintf(stderr, "[[reducer]] error on sem_post");
        exit(1);
    }
}




typedef std::pair<IntermediateVec::iterator, IntermediateVec::iterator> vecPointer;

/**
 * Intermediate vectors Comparator to get min value in the vectors
 */
struct IntermediateVecComparator {
    // this function asume both vecPointers are not at the end of the vector
    bool operator()(vecPointer * i, vecPointer * j)
    {
        return (*(* i->first).first) < (*(* j->first).first);
    }
} IntermediateVecComparator;


/**
 * checks if the tow K@ are identical
 * @param key2
 * @param iterator
 * @return
 */
bool same_K2_value(K2 * key2, const vecPointer & iterator) {
    if(iterator.first == iterator.second){
        return false;
    }
    K2 * second = ((* iterator.first).first);
    return not (* key2 < * second ) and not ( * second < * key2);
}


/**
 * create set of pair of iterators to the beginning spot and the end of each vector
 * then, in each iteration, get the min value of all the beginning iterators, collect the elements equal to
 * this min value, and take the iterator forward.
 * if the iterator reach the end of the vector, I added it to vector in order to delete it after finishing the for loop
 * (in order not to break the for iterator)
 * @param manager manager
 */
void shuffle(JobManager * manager){

    std::set<vecPointer *> allVectorsIterator;
    std::vector<vecPointer *> to_release;

    for (ThreadContext * context : manager->allThreads){
        if (! context->intermediateVec.empty()){
            allVectorsIterator.insert(new vecPointer( context->intermediateVec.begin(), context->intermediateVec.end()) );
        }
    }

    while(! allVectorsIterator.empty()){
        auto it = std::min_element(allVectorsIterator.begin(), allVectorsIterator.end(), IntermediateVecComparator);
        K2 * min_value = (*(* it)->first).first;
        std::vector<IntermediatePair> * cur_vector = new std::vector<IntermediatePair>;

        for(auto cur_iterator = allVectorsIterator.begin(); cur_iterator != allVectorsIterator.end() ; ){

            while (same_K2_value(min_value, *(* cur_iterator)) ){
                cur_vector->push_back(IntermediatePair(* (**cur_iterator).first) ) ;
                (**cur_iterator).first ++;
            }

            if ((*cur_iterator)->first == (*cur_iterator)->second ){
                to_release.push_back( (*cur_iterator) );
            }
            cur_iterator ++;
        }
        for( vecPointer * p : to_release){
            allVectorsIterator.erase(p);
            delete p;
        }
        to_release.clear();

        lock(& manager->vector_insertion_mutex);
        manager->vectors_to_reduce.push_back(cur_vector);
        unlock(& manager->vector_insertion_mutex);

        if (sem_post(& manager->semaphore) != 0){
            fprintf(stderr, "[[shuffler]] error on sem_post");
            exit(1);
        }
    }
    manager->dune_shuffle_phase = true;
}


/**
 * the thread function
 * @param arg the manager of the threads
 * @return nullptr
 */
void* taskPool(void* arg)
{
    ThreadContext * threadContext = (ThreadContext *)arg;
    mapProcess(threadContext);
    threadContext->manager->barrier->barrier();

    bool is_shuffler = false;
    // lock while changing the job stage - only one thread will do so and become the shuffler
    lock(& threadContext->manager->get_data_mutex);

    if (threadContext->manager->jobState.jobStage == MAP_STAGE)
    {
        // this is the first thread to reach this line so it is the shuffler
        is_shuffler = true;

        threadContext->manager->jobState.jobStage = REDUCE_STAGE;
        threadContext->manager->jobState.atomic_key_processed = 0;  // restart the counter
        threadContext->manager->jobState.key_to_process = 0;
        for (ThreadContext * context : threadContext->manager->allThreads)
        {
            threadContext->manager->jobState.key_to_process += context->intermediateVec.size();
        }
        if (threadContext->manager->jobState.key_to_process == 0){
            std::cerr << "no intermediate pairs found\n";
            exit(1);
        }

    }
    unlock(& threadContext->manager->get_data_mutex);

    if (is_shuffler)
    {
        shuffle(threadContext->manager);
    }

    reduceProcess(threadContext->manager);
    return nullptr;
}

/**
 * to insert to the thread's vector
 * @param key
 * @param value
 * @param context
 */
void emit2 (K2* key, V2* value, void* context)
{
    ( (ThreadContext *)context )->intermediateVec.push_back(IntermediatePair(key, value));
}


/**
 * to insert to the output vector
 * @param key
 * @param value
 * @param context
 */
void emit3 (K3* key, V3* value, void* context){
    auto manager = (JobManager *)context;
    lock( & manager->vector_insertion_mutex );
    ((JobManager *)context)->OutputVector.push_back(OutputPair(key, value));
    unlock(& manager->vector_insertion_mutex );
}


/***
 * start the all job
 * @param client
 * @param inputVec
 * @param outputVec
 * @param multiThreadLevel - num of thread it wants to create
 * @return pointer to the job manager
 */
JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
{
    if (multiThreadLevel < 1 or inputVec.empty())
    {
        std::cerr << "illegal number of threads or empty input vector\n";
        exit(1);
    }

    JobManager * manager = new JobManager(client, inputVec, outputVec, multiThreadLevel);

    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext * contexts = new ThreadContext{manager, 0};
        pthread_create(& contexts->pthread_identifier, NULL, taskPool, contexts);
        manager->allThreads.push_back(contexts);
    }
    return manager;
}


/**
 * make join for all the threads
 * @param job
 */
void waitForJob(JobHandle job){
    auto manager = (JobManager *)job;
    for (ThreadContext * i : manager->allThreads){
        pthread_join( i->pthread_identifier, nullptr);
        delete i;
    }
    manager->had_join = true;
}


/**
 *
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState* state){
    auto manager = (JobManager *)job;
    lock(& manager->get_data_mutex);
    state->stage = manager->jobState.jobStage;
    state->percentage = (float)manager->jobState.atomic_key_processed / manager->jobState.key_to_process * 100;
    unlock(& manager->get_data_mutex);
}


/**
 * closes the job
 * @param job
 */
void closeJobHandle(JobHandle job){
    auto manager = (JobManager *)job;
    if (! manager->had_join){
        waitForJob(job);
    }
    delete (JobManager *)job;
}

