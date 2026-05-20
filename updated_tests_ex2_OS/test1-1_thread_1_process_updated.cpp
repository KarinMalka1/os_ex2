#include <iostream>
#include <memory>
#include <vector>
#include <random>
#include <algorithm>
#include "MapReduceClient.h"
#include "MapReduceJob.h"
#include "MapContext.h"
#include "ReduceContext.h"

unsigned int unique_keys = 100;

class elements : public K1, public K2, public K3, public V1, public V2, public V3 {
public:
    elements(int i) : num(i) {}
    int num;

    // בגרסה שלך המפתחות מושווים דרך shared_ptr, אבל האופרטור בפנים נשאר דומה
    bool operator<(const K1 &other) const override {
        return num < static_cast<const elements &>(other).num;
    }
    bool operator<(const K2 &other) const override {
        return num < static_cast<const elements &>(other).num;
    }
    bool operator<(const K3 &other) const override {
        return num < static_cast<const elements &>(other).num;
    }
};

class tester : public MapReduceClient {
public:
    void map(const std::shared_ptr<K1> key, const std::shared_ptr<V1> value, MapContext &context) const override {
        int input = (static_cast<const elements*>(key.get())->num) % unique_keys;
        
        context.addIntermediate(std::make_shared<elements>(input), std::make_shared<elements>(1));
    }

    void reduce(const IntermediateVec &pairs, ReduceContext &context) const override {
        int count = static_cast<int>(pairs.size());
        int keyNum = static_cast<const elements*>(pairs.at(0).first.get())->num;
        
        // שימוש בשם הנכון מהקוד שלך: addOutput
        context.addOutput(std::make_shared<elements>(keyNum), std::make_shared<elements>(count));
    }
};

int main() {
    int numOfThreads = 4;
    InputVec inputVec;
    tester client;

    for (int j = 0; j < 100000; ++j) {
        inputVec.push_back({ std::make_shared<elements>(std::rand()), nullptr });
    }

    // הרצת ה-Job
    MapReduceJob job(client, inputVec, numOfThreads);

    // קבלת התוצאות
    OutputVec out = job.getOutput();

    for (auto &p : out) {
        // שימוש ב-get() כדי להגיע למצביע הגולמי מתוך ה-shared_ptr
        int key = static_cast<elements*>(p.first.get())->num;
        int count = static_cast<elements*>(p.second.get())->num;
        std::cout << "Key: " << key << "\t Count: " << count << "\n";
    }

    // אין צורך בלופים של delete בסוף - ה-shared_ptr מטפל בהכל אוטומטית!
    
    return 0;
}