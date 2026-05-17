#ifndef MAPREDUCECLIENT_H
#define MAPREDUCECLIENT_H

/*
	You CAN NOT change this header.
*/
#include "MapContext.h"
#include "ReduceContext.h"
#include "MapReduceKeys.h"

class MapReduceClient
{
public:
	/*
		Gets a single pair (K1, V1) and calls emit2(K2,V2, context) any number of times to output (K2, V2) pairs.
	*/
	virtual void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext &context) const = 0;

	// gets a single K2 key and a vector of all its respective V2 values
	// calls emit3(K3, V3, context) any number of times (usually once)
	// to output (K3, V3) pairs.
	virtual void reduce(const IntermediateVec &pairs, ReduceContext &context) const = 0;
};


#endif //MAPREDUCECLIENT_H
