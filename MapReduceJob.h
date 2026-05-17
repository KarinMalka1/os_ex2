#ifndef MAP_REDUCE_JOB_H
#define MAP_REDUCE_JOB_H

#include "MapReduceClient.h"
#include <vector>
#include <barrier>
#include <mutex>
#include <thread>
#include <atomic>
#include <queue>


// you can add other includes here

enum MapReduceStage
{
	UNDEFINED_STAGE, // 0
	MAP_STAGE, // 1
	SHUFFLE_STAGE, // 2
	REDUCE_STAGE // 3
};

class MapReduceState
{
public:
	MapReduceStage stage;
	double percentage;

	inline bool operator==(const MapReduceState &other) const
	{
		return this->stage == other.stage && std::abs(this->percentage - other.percentage) < 1e-6;
	}

	inline bool operator!=(const MapReduceState &other) const
	{
		return !(*this == other);
	}
};

class MapReduceJob
{
public:
	/*
	You CAN NOT change or add properties to this part (public API).
	*/

	MapReduceJob(const MapReduceClient &client, const InputVec &inputVec, int multiThreadLevel);

	~MapReduceJob();

	MapReduceState getState(void) const;

	bool isDone(void) const;
	
	void wait(void);

	OutputVec getOutput(void);

private:
    void threadWorker(int threadId, const MapReduceClient& client, const InputVec& inputVec);
    void updateState(MapReduceStage stage, uint32_t total, uint32_t processed);

	void runMapStage(int threadId, const MapReduceClient& client, const InputVec& inputVec);
    void runSortStage(int threadId);
    void runShuffleStage(int threadId);
    void runReduceStage(const MapReduceClient& client);

    std::vector<std::thread> threads;
    std::vector<MapContext> contexts;
    OutputVec outputVec;

    std::atomic<uint64_t> state;
    std::atomic<size_t> inputIndex;
    std::atomic<bool> isJoined;

    std::barrier<> barrier1;
    std::barrier<> barrier2;

    std::queue<IntermediateVec> shuffleQueue;
    std::mutex queueMutex;
    std::mutex outputMutex;
    std::mutex waitMutex;
};
	
#endif // MAP_REDUCE_JOB_H
