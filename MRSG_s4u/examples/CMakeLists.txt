cmake_minimum_required(VERSION 3.10)
project(MRSG_s4u)

set(CMAKE_CXX_STANDARD 11)

include_directories(../include
        /home/gabriel/simgrid/include)

add_subdirectory(../)


add_executable(hello_mrsg hello_mrsg.cpp)

target_link_libraries(hello_mrsg libmrsg.a simgrid)

set_target_properties(hello_mrsg PROPERTIES COMPILE_FLAGS "-Wall -g3" LINK_FLAGS "-lm -lsimgrid")