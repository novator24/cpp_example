cmake_minimum_required(VERSION 3.10)
project(WordFrequencyCounter)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Optimize for speed
set(CMAKE_CXX_FLAGS "-O3 -march=native -flto -DNDEBUG")

add_executable(freq src/freq.cpp)