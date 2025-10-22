cmake_minimum_required(VERSION 3.15)
project(s3_bench_cpp LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(AWSSDK REQUIRED COMPONENTS s3-crt s3 core)

add_executable(s3_bench main.cpp)
target_compile_options(s3_bench PRIVATE -O3 -Wall -Wextra -Wpedantic)
target_link_libraries(s3_bench PRIVATE ${AWSSDK_LINK_LIBRARIES})

add_executable(s3_bench_async async.cpp)
target_compile_options(s3_bench_async PRIVATE -O3 -Wall -Wextra -Wpedantic)
target_link_libraries(s3_bench_async PRIVATE ${AWSSDK_LINK_LIBRARIES})