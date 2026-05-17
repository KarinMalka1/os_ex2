#ifndef MAP_REDUCE_KEYS_H
#define MAP_REDUCE_KEYS_H

#include <vector>
#include <memory>
#include <utility>

// You CAN NOT modify this header.
 
// input key and value.
// the key, value for the map function and the MapReduceFramework
class K1
{
public:
	virtual ~K1() = default;
	virtual bool operator<(const K1 &other) const = 0;
};

class V1
{
public:
	virtual ~V1() = default;
};

// intermediate key and value.
// the key, value for the Reduce function created by the Map function
class K2
{
public:
	virtual ~K2() = default;
	virtual bool operator<(const K2 &other) const = 0;
};

class V2
{
public:
	virtual ~V2() = default;
};

// output key and value
// the key,value for the Reduce function created by the Map function
class K3
{
public:
	virtual ~K3() = default;
	virtual bool operator<(const K3 &other) const = 0;
};

class V3
{
public:
	virtual ~V3() = default;
};

using InputPair = std::pair<std::shared_ptr<K1>, std::shared_ptr<V1>>;
using IntermediatePair = std::pair<std::shared_ptr<K2>, std::shared_ptr<V2>>;
using OutputPair = std::pair<std::shared_ptr<K3>, std::shared_ptr<V3>>;

using InputVec = std::vector<InputPair>;
using IntermediateVec = std::vector<IntermediatePair>;
using OutputVec = std::vector<OutputPair>;

#endif // MAP_REDUCE_KEYS_H