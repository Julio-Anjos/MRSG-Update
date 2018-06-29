cmake_minimum_required(VERSION 3.10)
project(MRSG_s4u)

set(CMAKE_CXX_STANDARD 11)

include_directories(include
        /home/gabriel/simgrid/include)

set(SOURCE_FILES
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

add_library(mrsg ${SOURCE_FILES})

target_link_libraries(mrsg simgrid)

set_target_properties(mrsg PROPERTIES COMPILE_FLAGS "-Wall -g3" LINK_FLAGS "-lm -lsimgrid")


add_executable(hello_mrsg
        examples/hello_mrsg.cpp)

target_link_libraries(hello_mrsg mrsg)

set_target_properties(hello_mrsg PROPERTIES COMPILE_FLAGS "-Wall -g3" LINK_FLAGS "-Wall -g3")
