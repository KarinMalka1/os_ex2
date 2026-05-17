#include "MapContext.h"

// implement here your constructor and destructor
MapContext::MapContext()
{
}

MapContext::~MapContext()
{
}

void MapContext::addIntermediate(std::shared_ptr<K2> key, std::shared_ptr<V2> value)
{
    // TODO: implement this function
    intermediateVec.push_back(std::make_pair(key, value));

}


IntermediateVec& MapContext::getIntermediateVec()
{
    return intermediateVec;
}