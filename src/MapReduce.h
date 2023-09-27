#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

class MapReduce {
public:
    MapReduce(const std::string& filename, int mnum, int rnum)
        : filename(filename)
        , mnum(mnum)
        , rnum(rnum)
        , mapCount(0)
        , mapFinished(false)
        , reduceCount(0)
    {}
    void run();

private:
    void splitFile();
    size_t fileSize();
    void map(size_t sectionIndex);
    void reduce(size_t reduceIndex);

private:
    std::string filename;
    size_t mnum;
    size_t rnum;
    std::vector<std::string> sections;
    std::vector<std::vector<std::string>> mapResults;
    std::vector<std::vector<std::string>> reduceResults;
    std::mutex mutex;
    std::condition_variable cv;
    size_t mapCount;
    bool mapFinished;
    size_t reduceCount;
};
