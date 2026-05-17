#include "MapReduceJob.h"
#include <iostream>
#include <string>
#include <array>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <algorithm>

/*
=========================================
Wrappers for the K1, V1, K2, V2, K3, V3 classes
=========================================
*/
class VString : public V1 {
public:
	VString(std::string content) : content(content) { }
	std::string content;
};

class KChar : public K2, public K3
{
public:
	KChar(char c): c(c) {}
	virtual bool operator<(const K2 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	virtual bool operator<(const K3 &other) const {
		return c < static_cast<const KChar&>(other).c;
	}
	char c;
};

class VCount : public V2, public V3
{
public:
	VCount(int count): count(count) {}
	int count;
};


class CounterClient : public MapReduceClient {
public:
	void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext &context) const override
	{
		std::array<int, 256> counts;
		counts.fill(0);
		for(const char &c : std::dynamic_pointer_cast<const VString>(value)->content)
		{
			counts[(unsigned char) c]++;
		}

		for(int i = 0; i < 256; ++i) 
		{
			if(counts[i] == 0)
			{
				continue;
			}

			std::shared_ptr<KChar> k2 = std::make_shared<KChar>(i);
			std::shared_ptr<VCount> v2 = std::make_shared<VCount>(counts[i]);
			std::this_thread::sleep_for(std::chrono::milliseconds(150));
			context.addIntermediate(k2, v2);
		}
	}

	virtual void reduce(const IntermediateVec &pairs, ReduceContext &context) const override
	{
		const char c = std::dynamic_pointer_cast<const KChar>(pairs.at(0).first)->c;
		int count = 0;
		for(const IntermediatePair& pair: pairs) {
			count += std::dynamic_pointer_cast<const VCount>(pair.second)->count;
		}
		std::shared_ptr<KChar> k3 = std::make_shared<KChar>(c);
		std::shared_ptr<VCount> v3 = std::make_shared<VCount>(count);
		std::this_thread::sleep_for(std::chrono::milliseconds(150));
		context.addOutput(k3, v3);
	}
};

/*
=========================================
Main function
=========================================
*/
int main(int argc, char *argv[])
{
	if(argc != 2)
	{
		std::cerr << "Usage: " << argv[0] << " <number of threads>" << std::endl;
		return 1;
	}
	int numThreads = std::stoi(argv[1]);
	if(numThreads <= 0)
	{
		std::cerr << "Number of threads must be greater than 0" << std::endl;
		return 1;
	}
	CounterClient client;
	InputVec inputVec;
	OutputVec outputVec;
	VString s1("This string is full of characters");
	VString s2("Multithreading is awesome");
	VString s3("race conditions are bad");
	inputVec.push_back({nullptr, std::make_shared<VString>(s1)});
	inputVec.push_back({nullptr, std::make_shared<VString>(s2)});
	inputVec.push_back({nullptr, std::make_shared<VString>(s3)});
	MapReduceState state;
    MapReduceState last_state = {UNDEFINED_STAGE, 0};
	MapReduceJob job(client, inputVec, numThreads);
	state = job.getState();
    
	while(not job.isDone())
	{
		if(last_state != state)
		{
			std::cout << "stage " << state.stage << ", " << state.percentage << "% \n";
        }
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
        last_state = state;
		state = job.getState();
	}
	std::cout << "stage " << state.stage << ", " << state.percentage << "% \n";
	std::cout << "Done!\n";

	OutputVec output = job.getOutput();

	for(OutputPair &pair: output)
	{
		char c = std::dynamic_pointer_cast<const KChar>(pair.first)->c;
		int count = std::dynamic_pointer_cast<const VCount>(pair.second)->count;
		std::cout << "The character " << c << " appears " << count << " time" << (count > 1 ? "s" : "") << "\n";
	}
	
	return 0;
}

