#include "MapReduce.h"

void MapReduce::run() {
    splitFile();
    std::vector<std::thread> mapThreads;
    for (size_t i = 0; i < mnum; i++) {
        mapThreads.emplace_back(&MapReduce::map, this, i);
    }

    std::vector<std::thread> reduceThreads;
    for (size_t i = 0; i < rnum; i++) {
        reduceThreads.emplace_back(&MapReduce::reduce, this, i);
    }

    for (auto& thread : mapThreads) {
        thread.join();
    }

    for (auto& thread : reduceThreads) {
        thread.join();
    }
}

void MapReduce::splitFile() {
    std::ifstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Error opening file");
    }

    std::string line;
    std::string section;
    while (std::getline(file, line)) {
        section += line + "\n";
        if (section.size() >= (fileSize() / mnum)) {
            sections.push_back(section);
            section.clear();
        }
    }
    if (!section.empty()) {
        sections.push_back(section);
    }

    file.close();
}

size_t MapReduce::fileSize() {
    std::ifstream file(filename, std::ifstream::ate | std::ifstream::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Error opening file");
    }
    return file.tellg();
}

void MapReduce::map(size_t sectionIndex) {
    std::vector<std::string> result;
    std::istringstream iss(sections[sectionIndex]);
    std::string line;
    while (std::getline(iss, line)) {
        result.push_back(line);
    }

    std::sort(result.begin(), result.end());

    std::lock_guard<std::mutex> lock(mutex);
    mapResults.push_back(result);
    mapCount++;
    if (mapCount == mnum) {
        mapFinished = true;
        cv.notify_all();
    }
}

void MapReduce::reduce(size_t reduceIndex) {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [this]() { return mapFinished; });

    std::vector<std::string> combined;
    for (const auto& mapResult : mapResults) {
        combined.insert(combined.end(), mapResult.begin(), mapResult.end());
    }
    std::sort(combined.begin(), combined.end());

    std::vector<std::string> result;
    int chunkSize = combined.size() / rnum;
    int startIndex = reduceIndex * chunkSize;
    int endIndex = (reduceIndex == rnum - 1) ? combined.size() : (reduceIndex + 1) * chunkSize;
    for (int i = startIndex; i < endIndex; i++) {
        result.push_back(combined[i]);
    }

    reduceResults.push_back(result);
    reduceCount++;
    if (reduceCount == rnum) {
        cv.notify_all();
    }
}
