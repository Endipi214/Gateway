cmake_minimum_required(VERSION 3.18..3.8)
project(Gateway LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SRC_DIR ${CMAKE_SOURCE_DIR}/src)
set(INCLUDE_DIR ${CMAKE_SOURCE_DIR}/include)

file(GLOB SRC_FILES ${SRC_DIR}/*.c)
file(GLOB HEADER_FILES ${INCLUDE_DIR}/*.h )

add_executable(${PROJECT_NAME} ${SRC_FILES})
# Include directories
target_include_directories(${PROJECT_NAME} PRIVATE ${INCLUDE_DIR})

# Architecture flags
target_compile_options(${PROJECT_NAME} PRIVATE -march=native)

# Define _GNU_SOURCE for all source files
target_compile_definitions(${PROJECT_NAME} PRIVATE _GNU_SOURCE)

# Link libraries
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads OpenSSL::SSL OpenSSL::Crypto)
