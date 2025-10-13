cmake_minimum_required(VERSION 3.15)
project(s3_bench_cpp LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(AWSSDK REQUIRED COMPONENTS s3-crt s3 core)

add_executable(s3_bench main.cpp)
target_compile_options(s3_bench PRIVATE -O3 -Wall -Wextra)
target_link_libraries(s3_bench PRIVATE aws-cpp-sdk-s3-crt aws-cpp-sdk-s3 aws-cpp-sdk-core)
