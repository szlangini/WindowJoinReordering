# src/CMakeLists.txt

set(SOURCES
    Stream.cpp
    Window.cpp
    WindowJoinOperator.cpp
    SlidingWindowJoin.cpp
    IntervalJoin.cpp
    ResultEvaluator.cpp
    JoinPlan.cpp
    JoinOrderer.cpp
    Utils.cpp
    # Add other source files here
)

add_library(WindowJoinLib ${SOURCES})

# Optionally specify the include directories
target_include_directories(WindowJoinLib PUBLIC ${PROJECT_SOURCE_DIR}/include)