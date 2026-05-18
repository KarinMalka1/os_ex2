#include "MapReduceJob.h"
#include "ReduceContext.h"
#include <barrier>
#include <vector>
#include <algorithm>
/*
===============================================
Implement:
===============================================
*/
void MapReduceJob::updateState(MapReduceStage stage, uint32_t total, uint32_t processed)
{
    uint64_t new_state = (static_cast<uint64_t>(stage) << 62) |
                         (static_cast<uint64_t>(total) << 31) |
                         static_cast<uint64_t>(processed);
    state.store(new_state); 
}

void MapReduceJob::runMapStage(int threadId, const MapReduceClient& client, const InputVec& inputVec)
{
    while (true)
    {
        size_t currIndex = inputIndex.fetch_add(1);
        if (currIndex >= inputVec.size()) 
        {
            break;
        }
        client.map(inputVec[currIndex].first, inputVec[currIndex].second, contexts[threadId]);
        state.fetch_add(1);
    }
}

void MapReduceJob::runSortStage(int threadId)
{
    std::sort(contexts[threadId].getIntermediateVec().begin(), contexts[threadId].getIntermediateVec().end(),
              [](const IntermediatePair& a, const IntermediatePair& b) {
                  return *(a.first) < *(b.first);
              });

    barrier1.arrive_and_wait();
}

void MapReduceJob::runShuffleStage(int threadId)
{
    if (threadId != 0)
    {
        barrier2.arrive_and_wait();
        return;
    }

    uint32_t totalIntermediates = 0;
    for (auto& ctx : contexts) 
    {
        totalIntermediates += ctx.getIntermediateVec().size();
    }
    
    updateState(SHUFFLE_STAGE, totalIntermediates, 0);

    while (true)
    {
        std::shared_ptr<K2> maxKey = nullptr;
        for (auto& ctx : contexts)
        {
            if (!ctx.getIntermediateVec().empty())
            {
                auto& lastKey = ctx.getIntermediateVec().back().first;
                if (maxKey == nullptr || *maxKey < *lastKey) 
                {
                    maxKey = lastKey;
                }
            }
        }

        if (maxKey == nullptr) 
        {
            break;
        }

        IntermediateVec currentKeyBatch;
        for (auto& ctx : contexts)
        {
            auto& vec = ctx.getIntermediateVec();
            while (!vec.empty() && !(*maxKey < *(vec.back().first)) && !(*(vec.back().first) < *maxKey))
            {
                currentKeyBatch.push_back(vec.back());
                vec.pop_back();
            }
        }
        
        uint32_t batchSize = static_cast<uint32_t>(currentKeyBatch.size());
        shuffleQueue.push(std::move(currentKeyBatch));
        state.fetch_add(batchSize);
    }
    
    updateState(REDUCE_STAGE, totalIntermediates, 0);
    barrier2.arrive_and_wait();
}

void MapReduceJob::runReduceStage(const MapReduceClient& client)
{
    ReduceContext reduceCtx(outputVec, outputMutex);
    while (true)
    {
        IntermediateVec batchToReduce;
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (shuffleQueue.empty()) 
            {
                break;
            }
            batchToReduce = std::move(shuffleQueue.front());
            shuffleQueue.pop();
        }

        uint32_t batchSize = static_cast<uint32_t>(batchToReduce.size());
        client.reduce(batchToReduce, reduceCtx);
        state.fetch_add(batchSize);
    }
}

void MapReduceJob::threadWorker(int threadId, const MapReduceClient& client, const InputVec& inputVec)
{
    runMapStage(threadId, client, inputVec);
    runSortStage(threadId);
    runShuffleStage(threadId);
    runReduceStage(client);
}

MapReduceJob::MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel)
: contexts(multiThreadLevel),
  barrier1(multiThreadLevel),
  barrier2(multiThreadLevel) 
{
    updateState(MAP_STAGE, static_cast<uint32_t>(inputVec.size()), 0);

    inputIndex.store(0); 
    isJoined.store(false); 

    threads.reserve(multiThreadLevel);

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        threads.emplace_back(&MapReduceJob::threadWorker, this, i, std::ref(client), std::ref(inputVec));
    }
}


MapReduceState MapReduceJob::getState(void) const
{
    uint64_t currState = state.load();
    
    MapReduceStage stage = static_cast<MapReduceStage>(currState >> 62);
    uint32_t total = static_cast<uint32_t>((currState >> 31) & 0x7FFFFFFF);
    uint32_t processed = static_cast<uint32_t>(currState & 0x7FFFFFFF);

    double percentage = 0.0;
    if (total > 0)
    {
        percentage = (static_cast<double>(processed) / total) * 100.0;
    }
    
    return MapReduceState{stage, percentage};
}

void MapReduceJob::wait(void)
{
    // while (!isJoined){};
    // return;
    std::lock_guard<std::mutex> lock(waitMutex);
    
    if (isJoined.load())
    {
        return;
    }
    
    for (auto& thread : threads)
    {
        if (thread.joinable())
        {
            thread.join(); 
        }
    }
    
    isJoined.store(true);
}

OutputVec MapReduceJob::getOutput(void)
{
    // return MapReduceState::stage;
    wait();

    std::sort(outputVec.begin(), outputVec.end(),
              [](const OutputPair& a, const OutputPair& b) {
                  return *(a.first) < *(b.first);
              });
              
    return outputVec;
}

bool MapReduceJob::isDone(void) const
{
    MapReduceState currState = getState();
    if (currState.stage == REDUCE_STAGE && currState.percentage >= 100.0){
        return true;
    }
    return false;
}

MapReduceJob::~MapReduceJob()
{
    wait(); //waiting for all the threads to die
}
