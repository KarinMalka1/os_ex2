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
        context.addOutput(std::make_shared<elements>(keyNum), std::make_shared<elements>(count));
    }
};

int main() {
    unsigned int numOfProcess = 4;   // מספר הג'ובים העצמאיים שירוצו במקביל
    unsigned int numOfThreads = 1;   // כמה תהליכונים פנימיים בכל ג'וב
    
    tester client;
    
    // וקטורים לאחסון הקלטים והג'ובים
    std::vector<InputVec> all_inputs(numOfProcess);
    std::vector<std::unique_ptr<MapReduceJob>> jobs;

    // 1. הכנת הקלטים (שומרים על 100,000 לכל ג'וב)
    for (unsigned int l = 0; l < numOfProcess; ++l) {
        std::srand(l); // הבטחת קלט שונה לכל ג'וב
        for (int j = 0; j < 100000; ++j) {
            int r = std::rand();
            all_inputs[l].push_back({ std::make_shared<elements>(r), nullptr });
        }
    }

    // 2. השקת הג'ובים במקביל
    // יצירת האובייקט בתוך המחסנית או ב-unique_ptr גורמת לו להתחיל לרוץ מיד
    for (unsigned int i = 0; i < numOfProcess; ++i) {
        jobs.push_back(std::make_unique<MapReduceJob>(client, all_inputs[i], numOfThreads));
    }

    // 3. איסוף תוצאות והדפסה
    for (unsigned int m = 0; m < numOfProcess; ++m) {
        // getOutput קוראת ל-wait פנימי, כך שזה ימתין לסיום הג'וב הספציפי
        OutputVec out = jobs[m]->getOutput();
        
        // הדפסה בפורמט שביקשת
        for (const auto &p : out) {
            int val = static_cast<elements*>(p.second.get())->num;
            std::cout << "thread " << m + 1 << " out:\t" << val << '\n';
        }
        
        // ניקוי ה-shared_ptr של הקלטים (לא חובה, אבל מומלץ)
        all_inputs[m].clear();
    }

    return 0;
}