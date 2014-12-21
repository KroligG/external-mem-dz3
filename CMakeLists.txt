cmake_minimum_required(VERSION 2.8.4)
project(external_mem_dz3)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES util.h util.cpp BigFile.h main.cpp)
add_executable(external_mem_dz3 ${SOURCE_FILES})