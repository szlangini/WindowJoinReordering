enable_testing()

# Include FetchContent module to download GTest if not found
include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/release-1.12.1.zip
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Download and make available googletest
FetchContent_MakeAvailable(googletest)

# List of test sources
set(TEST_SOURCES
    test_swj.cpp
    test_swj_reordering.cpp
    test_ivj.cpp
)

# Create the test executable
add_executable(runTests ${TEST_SOURCES})

# Link the test executable with WindowJoinLib and GTest
target_link_libraries(runTests PRIVATE WindowJoinLib GTest::gtest_main)

# Include GoogleTest for automatic test discovery
include(GoogleTest)
gtest_discover_tests(runTests)
