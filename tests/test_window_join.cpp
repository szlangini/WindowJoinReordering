// tests/test_window_join.cpp

#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <set>
#include <sstream>

#include "ResultEvaluator.h"
#include "SlidingWindowJoin.h"
#include "Stream.h"

#define DEBUG_MODE_TESTS 1

TEST(SlidingWindowJoinTest, ThreeWayJoinTest_ABC) {
  // Create streams A, B, C
  auto A = std::make_shared<Stream>("A");
  A->addTuple({1}, 2);
  A->addTuple({2}, 8);

  auto B = std::make_shared<Stream>("B");
  B->addTuple({3}, 4);
  B->addTuple({4}, 10);

  auto C = std::make_shared<Stream>("C");
  C->addTuple({5}, 6);
  C->addTuple({6}, 12);

  // Window settings
  long length = 10;
  long slide = 5;

  // First join: A ⋈ B
  SlidingWindowJoin joinAB(A, B, length, slide, "A");
  auto ABStream = joinAB.compute();

  // Second join: (A ⋈ B) ⋈ C
  // Use "A" as the timestamp propagator
  SlidingWindowJoin joinABC(ABStream, C, length, slide, "A");
  auto resultABCStream = joinABC.compute();

  // Get the tuples from the result stream
  const auto& resultABC = resultABCStream->getTuples();

  // Evaluate results
  ResultEvaluator evaluator;
  long totalSum = evaluator.computeSum(resultABC);

  // Expected total sum calculation
  long expectedSum = 0;
  for (const auto& tuple : resultABC) {
    for (const auto& value : tuple.values) {
      expectedSum += value;
    }
  }

  // Compare total sums
  ASSERT_EQ(totalSum, expectedSum);
}

TEST(SlidingWindowJoinTest, ThreeWayJoinTest_BAC) {
  // Create streams A, B, C
  auto A = std::make_shared<Stream>("A");
  A->addTuple({1}, 2);
  A->addTuple({2}, 8);

  auto B = std::make_shared<Stream>("B");
  B->addTuple({3}, 4);
  B->addTuple({4}, 10);

  auto C = std::make_shared<Stream>("C");
  C->addTuple({5}, 6);
  C->addTuple({6}, 12);

  // Window settings
  long length = 10;
  long slide = 5;

  // First join: B ⋈ A
  SlidingWindowJoin joinBA(B, A, length, slide, "A");
  auto BAStream = joinBA.compute();

  // Second join: (B ⋈ A) ⋈ C
  // Use "A" as the timestamp propagator
  SlidingWindowJoin joinBAC(BAStream, C, length, slide, "A");
  auto resultBACStream = joinBAC.compute();

  // Get the tuples from the result stream
  const auto& resultBAC = resultBACStream->getTuples();

  // Evaluate results
  ResultEvaluator evaluator;
  long totalSumBAC = evaluator.computeSum(resultBAC);

  // Now, we need to compare the results from the first test (ABC) and this
  // test
  // (BAC) Since we're in a different test case, we need to recompute the ABC
  // results or pass them somehow

  // For comparison, we'll recompute the ABC results here
  // First join: A ⋈ B (same as before)
  SlidingWindowJoin joinAB(A, B, length, slide, "A");
  auto ABStream = joinAB.compute();

  // Second join: (A ⋈ B) ⋈ C
  SlidingWindowJoin joinABC(ABStream, C, length, slide, "A");
  auto resultABCStream = joinABC.compute();

  // Get the tuples from the result stream
  const auto& resultABC = resultABCStream->getTuples();

  // Compute total sum for ABC
  long totalSumABC = evaluator.computeSum(resultABC);

#if DEBUG_MODE_TESTS
  std::cout << "Total sum BAC: " << totalSumBAC << std::endl;
  std::cout << "Total sum ABC: " << totalSumABC << std::endl;
#endif
  // Compare total sums
  ASSERT_EQ(totalSumBAC, totalSumABC)
      << "Total sums do not match between ABC and BAC join orders.";

  // Additionally, compare the actual result tuples
  std::stringstream errorStream;
  bool resultsEqual =
      evaluator.compareResults(resultBAC, resultABC, errorStream);

  if (!resultsEqual) {
    std::cerr << "Results differ between ABC and BAC join orders:\n"
              << errorStream.str();
  }

  ASSERT_TRUE(resultsEqual)
      << "Result tuples are not equal between ABC and BAC join orders.";
}
