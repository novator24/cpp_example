cmake_minimum_required(VERSION 3.10)
project(WordFrequencyCounter)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Optimize for speed
set(CMAKE_CXX_FLAGS "-O3 -march=native -flto -DNDEBUG -pthread")

# If TBB is available, it can be used for parallel algorithms
find_package(TBB QUIET)
if(TBB_FOUND)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ltbb")
endif()

add_executable(freq src/freq.cpp)
target_link_libraries(freq pthread)
