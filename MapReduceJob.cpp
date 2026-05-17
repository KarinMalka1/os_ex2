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
        IntermediateVec currentBatch;
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (shuffleQueue.empty()) 
            {
                break;
            }
            currentBatch = std::move(shuffleQueue.front());
            shuffleQueue.pop();
        }

        uint32_t batchSize = static_cast<uint32_t>(currentBatch.size());
        client.reduce(currentBatch, reduceCtx);
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
    // TODO: implement this function
}

void MapReduceJob::wait(void)
{
    // TODO: implement this function
}

OutputVec MapReduceJob::getOutput(void)
{
    // TODO: implement this function
}

bool MapReduceJob::isDone(void) const
{
    // TODO: implement this function
}

MapReduceJob::~MapReduceJob()
{
    // TODO: implement this destructor
}
