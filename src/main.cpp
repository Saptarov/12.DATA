#include "MapReduce.h"

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <filename> <mnum> <rnum>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];
    int mnum = std::stoi(argv[2]);
    int rnum = std::stoi(argv[3]);

    try {
        MapReduce mapReduce(filename, mnum, rnum);
        mapReduce.run();
    } catch (std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
