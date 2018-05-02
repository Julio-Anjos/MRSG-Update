cmake_minimum_required(VERSION 3.10)
project(MRSG_s4u)

set(CMAKE_CXX_STANDARD 11)

include_directories(include)
include_directories( "/home/gabriel/simgrid/include",
        "/home/gabriel/simgrid/src")

add_executable(MRSG_s4u
        examples/hello_mrsg.cpp
        include/common_mrsg.hpp
        include/dfs_mrsg.h
        include/mrsg.h
        include/task_mrsg.hpp
        include/worker_mrsg.h
        src/common_mrsg.cpp
        src/dfs_mrsg.cpp
        src/master_mrsg.cpp
        src/simcore_mrsg.cpp
        src/task_mrsg.cpp
        src/user_mrsg.cpp
        src/worker_mrsg.cpp)
