#include "MapReduceJob.h"
#include <iostream>
#include <sstream>
#include <string>

class StringValue : public V1
{
public:
    std::string str;
    StringValue(const std::string &s) : str(s) {}
};

class StringKey : public K2, public K3
{
public:
    std::string str;
    StringKey(const std::string &s) : str(s) {}
    bool operator<(const K2 &other) const override
    {
        return str < dynamic_cast<const StringKey&>(other).str;
    }
    bool operator<(const K3 &other) const override
    {
        return str < dynamic_cast<const StringKey&>(other).str;
    }
};

class IntValue : public V2, public V3
{
public:
    int num;
    IntValue(int n) : num(n) {}
};

class WordCountClient : public MapReduceClient
{
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value,
             MapContext &context) const override
    {
        auto sv = std::static_pointer_cast<StringValue>(value);
        std::istringstream iss(sv->str);
        std::string word;
        while(iss >> word)
        {
            context.addIntermediate(std::make_shared<StringKey>(word),
                                    std::make_shared<IntValue>(1));
        }
    }
    void reduce(const IntermediateVec &pairs,
                ReduceContext &context) const override
    {
        auto key = std::static_pointer_cast<StringKey>(pairs[0].first);
        int count = 0;
        for(auto &p : pairs)
        {
            count += std::static_pointer_cast<IntValue>(p.second)->num;
        }
        context.addOutput(std::make_shared<StringKey>(key->str),
                          std::make_shared<IntValue>(count));
    }
};

int main()
{
    WordCountClient client;
    InputVec inputVec;
    inputVec.push_back({nullptr, std::make_shared<StringValue>("hello world hello")});
    inputVec.push_back({nullptr, std::make_shared<StringValue>("world foo bar")});
    inputVec.push_back({nullptr, std::make_shared<StringValue>("hello bar baz")});
    inputVec.push_back({nullptr, std::make_shared<StringValue>("foo baz hello")});
    inputVec.push_back({nullptr, std::make_shared<StringValue>("world world bar")});

    MapReduceJob job(client, inputVec, 4);
    auto output = job.getOutput();
    for(auto &p : output)
    {
        auto k = std::static_pointer_cast<StringKey>(p.first);
        auto v = std::static_pointer_cast<IntValue>(p.second);
        std::cout << "(" << k->str << ", " << v->num << ")" << std::endl;
    }
    return 0;
}
