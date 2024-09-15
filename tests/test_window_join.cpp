// tests/test_window_join.cpp

// Tests correctness of window joins.
#include <gtest/gtest.h>

#include <iostream>
#include <memory>
#include <set>
#include <sstream>

#include "JoinPlan.h"
#include "ResultEvaluator.h"
#include "SlidingWindowJoin.h"
#include "Stream.h"
#include "TimeDomain.h"

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
  SlidingWindowJoin joinAB(A, B, length, slide, TimeDomain::EVENT_TIME, "A");
  auto ABStream = joinAB.compute();

  // Second join: (A ⋈ B) ⋈ C
  // Use "A" as the timestamp propagator
  SlidingWindowJoin joinABC(ABStream, C, length, slide, TimeDomain::EVENT_TIME,
                            "A");
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

TEST(SlidingWindowJoinTest, ThreeWayJoinTest_BAC_ET) {
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
  SlidingWindowJoin joinBA(B, A, length, slide, TimeDomain::EVENT_TIME, "A");
  auto BAStream = joinBA.compute();

  // Second join: (B ⋈ A) ⋈ C
  // Use "A" as the timestamp propagator
  SlidingWindowJoin joinBAC(BAStream, C, length, slide, TimeDomain::EVENT_TIME,
                            "A");
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
  SlidingWindowJoin joinAB(A, B, length, slide, TimeDomain::EVENT_TIME, "A");
  auto ABStream = joinAB.compute();

  // Second join: (A ⋈ B) ⋈ C
  SlidingWindowJoin joinABC(ABStream, C, length, slide, TimeDomain::EVENT_TIME,
                            "A");
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

TEST(JoinPlanTest, JoinPlan_ABC) {
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
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::EVENT_TIME, "A");

  // Second join: (A ⋈ B) ⋈ C
  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, length, slide, TimeDomain::EVENT_TIME, "A");

  // Create a JoinPlan with the final join as the root
  JoinPlan planABC(joinABC);

  // Compute the final result
  auto resultStream = planABC.compute();

  // Get the tuples from the result stream
  const auto& result = resultStream->getTuples();

  // Evaluate results (manual sum check)
  long manualSum = 0;
  for (const auto& tuple : result) {
    for (const auto& value : tuple.getValues()) {
      manualSum += value;
    }
  }

  // Expected total sum calculation
  long expectedSum = 0;
  for (const auto& tuple : result) {
    for (const auto& value : tuple.getValues()) {
      expectedSum += value;
    }
  }

  // Compare total sums
  ASSERT_EQ(manualSum, expectedSum);
}

TEST(JoinPlanTest, JoinPlan_BAC) {
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
  auto joinBA = std::make_shared<SlidingWindowJoin>(
      B, A, length, slide, TimeDomain::EVENT_TIME, "A");

  // Second join: (B ⋈ A) ⋈ C
  auto joinBAC = std::make_shared<SlidingWindowJoin>(
      joinBA, C, length, slide, TimeDomain::EVENT_TIME, "A");

  // Create a JoinPlan with the final join as the root
  JoinPlan planBAC(joinBAC);

  // Compute the final result
  auto resultStream = planBAC.compute();

  // Get the tuples from the result stream
  const auto& result = resultStream->getTuples();

  // Evaluate results (manual sum check)
  long manualSum = 0;
  for (const auto& tuple : result) {
    for (const auto& value : tuple.getValues()) {
      manualSum += value;
    }
  }

  // Expected total sum calculation
  long expectedSum = 0;
  for (const auto& tuple : result) {
    for (const auto& value : tuple.getValues()) {
      expectedSum += value;
    }
  }

  // Compare total sums
  ASSERT_EQ(manualSum, expectedSum);
}

TEST(SlidingWindowJoinTest, ABC_CAB_PT_A2_TEST) {
  // Step 1: Create streams
  auto A = std::make_shared<Stream>("A");
  A->addTuple({1}, 2);
  A->addTuple({2}, 8);

  auto B = std::make_shared<Stream>("B");
  B->addTuple({3}, 4);
  B->addTuple({4}, 10);

  auto C = std::make_shared<Stream>("C");
  C->addTuple({5}, 6);
  C->addTuple({6}, 12);

  // Step 2: Define Window Settings for Case A2
  long length = 10;
  long slide = 10;  // Tumbling window since slide == length

  // Step 3: Create Initial JoinPlan for ABC using Processing Time
  auto joinAB_A2_PT = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::PROCESSING_TIME);
  auto joinABC_A2_PT = std::make_shared<SlidingWindowJoin>(
      joinAB_A2_PT, C, length, slide, TimeDomain::PROCESSING_TIME);
  auto initialPlanABC_A2_PT = std::make_shared<JoinPlan>(joinABC_A2_PT);

  // Step 4: Create Initial JoinPlan for CAB using Processing Time
  auto joinCA_A2_PT = std::make_shared<SlidingWindowJoin>(
      C, A, length, slide, TimeDomain::PROCESSING_TIME);
  auto joinCAB_A2_PT = std::make_shared<SlidingWindowJoin>(
      joinCA_A2_PT, B, length, slide, TimeDomain::PROCESSING_TIME);
  auto initialPlanCAB_A2_PT = std::make_shared<JoinPlan>(joinCAB_A2_PT);

  // Step 5: Compute Reference Result from Initial JoinPlan ABC
  ResultEvaluator evaluator;
  auto referenceStream_A2_PT = initialPlanABC_A2_PT->compute();
  const auto& referenceResult_A2_PT = referenceStream_A2_PT->getTuples();
  long referenceSum_A2_PT = evaluator.computeSum(referenceResult_A2_PT);

  // Step 6: Compute Result from Initial JoinPlan CAB
  auto resultStreamCAB_A2_PT = initialPlanCAB_A2_PT->compute();
  const auto& resultCAB_A2_PT = resultStreamCAB_A2_PT->getTuples();
  long resultSumCAB_A2_PT = evaluator.computeSum(resultCAB_A2_PT);

  // Step 7: Compare the Sums
  ASSERT_EQ(referenceSum_A2_PT, resultSumCAB_A2_PT)
      << "Results differ between ABC and CAB when using Processing Time.";

  // Step 8: Optionally, Compare the Actual Tuples
  std::stringstream errorStream;
  bool resultsEqual = evaluator.compareResults(referenceResult_A2_PT,
                                               resultCAB_A2_PT, errorStream);
  ASSERT_TRUE(resultsEqual)
      << "Result tuples differ between ABC and CAB with Processing Time:\n"
      << errorStream.str();
}