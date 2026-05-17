#include "ReduceContext.h"

// implement here your constructor and destructor
ReduceContext::ReduceContext(OutputVec& outputVecRef, std::mutex& outputMutexRef)
    : outputVec(outputVecRef), 
      outputMutex(outputMutexRef)
{
}

ReduceContext::~ReduceContext()
{
}

void ReduceContext::addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value)
{
    // TODO: implement this function
    std::lock_guard<std::mutex> lock(outputMutex);
    outputVec.push_back(std::make_pair(key, value));
}

