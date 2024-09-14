# CMakeLists.txt at the root

cmake_minimum_required(VERSION 3.10)
project(WindowJoinProject)

set(CMAKE_CXX_STANDARD 17)

# Include directories
include_directories(${PROJECT_SOURCE_DIR}/include)

# Add subdirectories
add_subdirectory(src)
add_subdirectory(tests)