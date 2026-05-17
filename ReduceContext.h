#ifndef REDUCE_CONTEXT_H
#define REDUCE_CONTEXT_H

#include "MapReduceKeys.h"

class ReduceContext
{
public:
    void addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value);

    /*
    You can change everything else, including the constructor/desturctor
    You can also add fields here (even public ones)
    */

    ReduceContext(OutputVec& outputVec, std::mutex& outputMutex);
    ~ReduceContext();

    void addOutput(std::shared_ptr<K3> key, std::shared_ptr<V3> value);

private:
    OutputVec& outputVec;
    std::mutex& outputMutex;
};


#endif // REDUCE_CONTEXT_H
