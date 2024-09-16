# Window Join Reordering and Interval Joins

## Overview

This project implements reordering of window joins (Sliding Window Joins and Interval Joins) in both **Event Time** and **Processing Time** (just Sliding Window Join) domains. The goal is to generate and validate all valid join permutations, ensuring correctness of the join plans.

We can compute joins for automatic stream generators and validate against other plans of the same join.

## Classes

- **Node**: Base class for both streams and join operators.
- **Stream**: Represents a stream of tuples, each with a timestamp and value.
- **WindowJoinOperator**: Base class for all windowed joins, managing common functionality such as time propagation.
- **SlidingWindowJoin**: Represents a sliding window join between two streams, supporting different time domains and window configurations.
- **IntervalJoin**: Implements an interval-based join, where windows are defined dynamically around each tuple based on lower and upper bounds.
- **JoinPlan**: Encapsulates a join tree and provides the `compute()` method to execute the join and return the results.
- **JoinOrderer**: Handles reordering of join plans by generating valid permutations of the join order and constructing new join plans.

## Tests

- The tests validate correctness and performance of various join configurations:
  - **SlidingWindowJoin Tests**: Verifies the join results and reordering in both event and processing time.
  - **IntervalJoin Tests**: Ensures correctness of interval-based joins with different bounds.
  - **Reordering Tests**: Validates the correctness of reordering the join plans and comparing the output sums across different permutations.
  
  The tests are implemented using **GoogleTest** and can be run to verify functionality.

## Building and Running Tests

### Prerequisites
- C++ Compiler (e.g., g++, clang++)
- CMake (version 3.10 or higher)
- GoogleTest (included via FetchContent)

### Build Instructions

1. **Clone the repository** and navigate to the project directory.
   ```bash
   git clone <repo-url>
   cd WindowJoinReordering
   ```

2. **Create a build directory** and navigate to it.
mkdir build
cd build

3. **Configure the build** using CMake
cmake ..

4. **Build the project**
cmake --build .

### Running Tests
./tests/runTests


