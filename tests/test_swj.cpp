// tests/test_swj.cpp

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
#include "Utils.h"

#define DEBUG_MODE 0

TEST(JoinPlanTest, JoinPlan_ABC_Compute_Sum_Test) {
  // Step 1: Create streams A, B, C using the helper function
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

#if DEBUG_MODE
  A->printTuples();
  B->printTuples();
  C->printTuples();
#endif

  // Step 2: Window settings
  long length = 10;
  long slide = 5;

  // Step 3: First join: A ⋈ B
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::EVENT_TIME, "A");

  auto resultStreamAB = joinAB->compute();
  auto resultAB = resultStreamAB->getTuples();

#if DEBUG_MODE
  std::cout << "Join results for AB (A ⋈ B):\n";
  for (const auto& tuple : resultAB) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 4: Second join: (A ⋈ B) ⋈ C
  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, length, slide, TimeDomain::EVENT_TIME, "A");

  // Step 5: Create a JoinPlan with the final join as the root
  JoinPlan planABC(joinABC);

  // Step 6: Compute the final result
  auto resultStream = planABC.compute();

  // Step 7: Get the tuples from the result stream
  const auto& result = resultStream->getTuples();

  // Step 8: Evaluate results (manual sum check)
  long manualSum = 0;
  for (const auto& tuple : result) {
    for (const auto& value : tuple.getValues()) {
      manualSum += value;
    }
  }

  // Expected total sum based on manually calculating the joined tuples:
  long expectedSum = 90;  // Pre-computed based on the created stream tuples
                          // and their combinations

  // Step 9: Compare total sums
  ASSERT_EQ(manualSum, expectedSum);
}

TEST(SlidingWindowJoinTest, ABC_CAB_ET_A1_TEST) {
  // Use the helper function to create streams A, B, C
  auto A = createStream("A", 100, linearValueDistribution, 100, 1);
  auto B = createStream("B", 200, linearValueDistribution, 100, 2);
  auto C = createStream("C", 30, linearValueDistribution, 50, 3);

#if DEBUG_MODE
  A->printTuples();
  B->printTuples();
  C->printTuples();
#endif

  // Define SlidingWindowJoin settings
  long length = 10;
  long slide = 5;

  // Join Plan 1: ABC (A ⋈ B ⋈ C) with "A" as the timestamp propagator
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::EVENT_TIME, "A");
  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, length, slide, TimeDomain::EVENT_TIME,
      joinAB->getTimestampPropagator());
  auto planABC = std::make_shared<JoinPlan>(joinABC);

  // Compute the result for ABC
  ResultEvaluator evaluator;
  auto resultStreamABC = planABC->compute();
  const auto& resultABC = resultStreamABC->getTuples();

#if DEBUG_MODE
  std::cout << "Join results for ABC (A ⋈ B ⋈ C):\n";
  for (const auto& tuple : resultABC) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Calculate the total sum for ABC
  long totalSumABC = evaluator.computeSum(resultABC);

  // Join Plan 2: CAB (C ⋈ A ⋈ B) with "A" as the timestamp propagator
  auto joinCA = std::make_shared<SlidingWindowJoin>(
      C, A, length, slide, TimeDomain::EVENT_TIME, "A");
  auto joinCAB = std::make_shared<SlidingWindowJoin>(
      joinCA, B, length, slide, TimeDomain::EVENT_TIME,
      joinCA->getTimestampPropagator());
  auto planCAB = std::make_shared<JoinPlan>(joinCAB);

  // Compute the result for CAB
  auto resultStreamCAB = planCAB->compute();
  const auto& resultCAB = resultStreamCAB->getTuples();

#if DEBUG_MODE
  std::cout << "Join results for CAB (C ⋈ A ⋈ B):\n";
  for (const auto& tuple : resultCAB) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Calculate the total sum for CAB
  long totalSumCAB = evaluator.computeSum(resultCAB);

  // Compare the total sums of ABC and CAB
  ASSERT_EQ(totalSumABC, totalSumCAB)
      << "Total sums do not match between ABC and CAB join orders.";

  // Optionally compare the actual result tuples
  std::stringstream errorStream;
  bool resultsEqual =
      evaluator.compareResults(resultABC, resultCAB, errorStream);

  ASSERT_TRUE(resultsEqual);
}

TEST(SlidingWindowJoinTest, ABC_CAB_ET_A2_TEST) {
  // Step 1: Create streams
  auto A = createStream("A", 100, linearValueDistribution, 100, 1);
  auto B = createStream("B", 100, linearValueDistribution, 200, 2);
  auto C = createStream("C", 200, linearValueDistribution, 100, 3);

  // Step 2: Define Window Settings for Case A2
  long length = 10;
  long slide = 10;  // Tumbling window since slide == length

  // Step 3: Create Initial JoinPlan for ABC using Event Time
  auto joinAB_A2_ET = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::EVENT_TIME, "A");
  auto joinABC_A2_ET = std::make_shared<SlidingWindowJoin>(
      joinAB_A2_ET, C, length, slide, TimeDomain::EVENT_TIME,
      joinAB_A2_ET->getTimestampPropagator());
  auto initialPlanABC_A2_ET = std::make_shared<JoinPlan>(joinABC_A2_ET);

  // Step 4: Create Initial JoinPlan for CAB using Event Time
  auto joinCA_A2_ET = std::make_shared<SlidingWindowJoin>(
      C, A, length, slide, TimeDomain::EVENT_TIME, "A");
  auto joinCAB_A2_ET = std::make_shared<SlidingWindowJoin>(
      joinCA_A2_ET, B, length, slide, TimeDomain::EVENT_TIME,
      joinCA_A2_ET->getTimestampPropagator());
  auto initialPlanCAB_A2_ET = std::make_shared<JoinPlan>(joinCAB_A2_ET);

  // Step 5: Compute Reference Result from Initial JoinPlan ABC
  ResultEvaluator evaluator;
  auto referenceStream_A2_ET = initialPlanABC_A2_ET->compute();
  const auto& referenceResult_A2_ET = referenceStream_A2_ET->getTuples();
  long referenceSum_A2_ET = evaluator.computeSum(referenceResult_A2_ET);

  // Step 6: Compute Result from Initial JoinPlan CAB
  auto resultStreamCAB_A2_ET = initialPlanCAB_A2_ET->compute();
  const auto& resultCAB_A2_ET = resultStreamCAB_A2_ET->getTuples();
  long resultSumCAB_A2_ET = evaluator.computeSum(resultCAB_A2_ET);

  // Step 7: Compare the Sums
  ASSERT_EQ(referenceSum_A2_ET, resultSumCAB_A2_ET)
      << "Results differ between ABC and CAB when using Event Time.";

  // Step 8: Optionally, Compare the Actual Tuples
  std::stringstream errorStream;
  bool resultsEqual = evaluator.compareResults(referenceResult_A2_ET,
                                               resultCAB_A2_ET, errorStream);
  ASSERT_TRUE(resultsEqual)
      << "Result tuples differ between ABC and CAB with Event Time:\n"
      << errorStream.str();
}

TEST(SlidingWindowJoinTest, ABC_CAB_PT_A2_TEST) {
  // Step 1: Create streams
  auto A = createStream("A", 1000, linearValueDistribution, 100, 1);
  auto B = createStream("B", 200, linearValueDistribution, 100, 2);
  auto C = createStream("C", 300, linearValueDistribution, 100, 3);

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

TEST(SlidingWindowJoinTest, ABC_CAB_ET_A3) {
  // Step 1: Create streams A, B, C using the helper function
  auto A = createStream("A", 8, linearValueDistribution, 100, 1);
  auto B = createStream("B", 8, linearValueDistribution, 100, 2);
  auto C = createStream("C", 8, linearValueDistribution, 100, 3);

  // Step 2: Define sliding window settings for W1 and W2 with overlaps
  // Case A3 W1 != W2, W2.s < W2.l and W1.l >= W2.l
  long W1_length = 20;  // Length of W1 (larger)
  long W1_slide = 30;   // Slide of W1 (smaller than its length)
  long W2_length = 15;  // Length of W2 (smaller)
  long W2_slide = 1;    // Slide of W2 (also smaller than its length)

  // Step 3: Create JoinPlan for ABC using SlidingWindowJoin with Event Time
  // A ⋈ B ⋈ C
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, W1_length, W1_slide, TimeDomain::EVENT_TIME, "A");

  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, W2_length, W2_slide, TimeDomain::EVENT_TIME,
      joinAB->getTimestampPropagator());

  // Create the initial JoinPlan for ABC
  auto initialPlanABC = std::make_shared<JoinPlan>(joinABC);

  // Step 4: Compute the initial JoinPlan (ABC)
  ResultEvaluator evaluator;
  auto resultStreamABC = initialPlanABC->compute();
  const auto& resultABC = resultStreamABC->getTuples();
  long resultSumABC = evaluator.computeSum(resultABC);

  // Step 5: Create JoinPlan for CAB using the same sliding window settings
  // C ⋈ A ⋈ B
  auto joinCA = std::make_shared<SlidingWindowJoin>(
      C, A, W2_length, W2_slide, TimeDomain::EVENT_TIME, "A");

  auto joinCAB = std::make_shared<SlidingWindowJoin>(
      joinCA, B, W1_length, W1_slide, TimeDomain::EVENT_TIME,
      joinCA->getTimestampPropagator());

  // Create the JoinPlan for CAB
  auto planCAB = std::make_shared<JoinPlan>(joinCAB);

  // Step 6: Compute the reordered JoinPlan (CAB)
  auto resultStreamCAB = planCAB->compute();
  const auto& resultCAB = resultStreamCAB->getTuples();
  long resultSumCAB = evaluator.computeSum(resultCAB);

  // Step 7: Compare the results
  ASSERT_EQ(resultSumABC, resultSumCAB)
      << "Result sums do not match between ABC and CAB join orders for Case "
         "A3.";

  // Optionally, you can compare the actual tuples between the two plans
  std::stringstream errorStream;
  bool resultsEqual =
      evaluator.compareResults(resultABC, resultCAB, errorStream);
  ASSERT_TRUE(resultsEqual) << "Result tuples are not equal between ABC and "
                               "CAB join orders for Case A3.\n"
                            << errorStream.str();
}

TEST(SlidingWindowJoinTest, ABC_CAB_ET_A4_TEST) {
  // Step 1: Create streams A, B, C using the helper function
  auto A = createStream("A", 10, linearValueDistribution, 100, 1);
  auto B = createStream("B", 10, linearValueDistribution, 100, 2);
  auto C = createStream("C", 10, linearValueDistribution, 100, 3);

  // Window settings
  long W1_length = 15;  // Length of W1
  long W2_length = 10;  // Length of W2
  long W2_slide = 10;   // Slide of W2 is equal to its length
  long W1_slide = 5;    // Slide of W1 is smaller than its length

  // First join: A ⋈ B with W1 and W2 (A ⋈ B) ⋈ C
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, W1_length, W1_slide, TimeDomain::EVENT_TIME, "A");

  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, W2_length, W2_slide, TimeDomain::EVENT_TIME,
      joinAB->getTimestampPropagator());

  // Create the initial JoinPlan
  auto initialPlan = std::make_shared<JoinPlan>(joinABC);

  // Step 2: Compute the initial JoinPlan ABC (A ⋈ B ⋈ C)
  ResultEvaluator evaluator;
  auto resultStreamABC = initialPlan->compute();
  const auto& resultABC = resultStreamABC->getTuples();
  long resultSumABC = evaluator.computeSum(resultABC);

  // Step 3: Create JoinPlan for CAB (C ⋈ A ⋈ B) with the same window properties
  auto joinCA = std::make_shared<SlidingWindowJoin>(
      C, A, W2_length, W2_slide, TimeDomain::EVENT_TIME, "A");

  auto joinCAB = std::make_shared<SlidingWindowJoin>(
      joinCA, B, W1_length, W1_slide, TimeDomain::EVENT_TIME,
      joinCA->getTimestampPropagator());

  auto planCAB = std::make_shared<JoinPlan>(joinCAB);

  // Step 4: Compute the reordered JoinPlan CAB
  auto resultStreamCAB = planCAB->compute();
  const auto& resultCAB = resultStreamCAB->getTuples();
  long resultSumCAB = evaluator.computeSum(resultCAB);

  // Step 5: Compare the results
  ASSERT_EQ(resultSumABC, resultSumCAB)
      << "Result sums do not match between ABC and CAB join orders.";

  // Optionally, you can compare the actual tuples between the two plans
  std::stringstream errorStream;
  bool resultsEqual =
      evaluator.compareResults(resultABC, resultCAB, errorStream);
  ASSERT_TRUE(resultsEqual)
      << "Result tuples are not equal between ABC and CAB join orders.\n"
      << errorStream.str();
}

TEST(SlidingWindowJoinTest, ABC_CAB_PT_A4_TEST) {
  // Step 1: Create streams A, B, C using the helper function
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define sliding window settings for W1 and W2 with different lengths
  // and slides
  long W1_length = 15;  // Length of W1 (larger window)
  long W2_length = 10;  // Length of W2 (smaller window)
  long W1_slide = 10;   // Slide of W1 (smaller than its length)
  long W2_slide = 5;    // Slide of W2 Divisor of w1 slide.

  // Step 3: Create JoinPlan for ABC using SlidingWindowJoin with Processing
  // Time A ⋈ B ⋈ C (with Processing Time as the domain)
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, W1_length, W1_slide, TimeDomain::PROCESSING_TIME, "NONE");

  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, W2_length, W2_slide, TimeDomain::PROCESSING_TIME, "NONE");

  // Create the initial JoinPlan for ABC
  auto initialPlanABC = std::make_shared<JoinPlan>(joinABC);

  // Step 4: Compute the initial JoinPlan (ABC)
  ResultEvaluator evaluator;
  auto resultStreamABC = initialPlanABC->compute();
  const auto& resultABC = resultStreamABC->getTuples();
  long resultSumABC = evaluator.computeSum(resultABC);

  // Step 5: Create JoinPlan for CAB using the same sliding window settings
  // C ⋈ A ⋈ B (with Processing Time as the domain)
  auto joinCA = std::make_shared<SlidingWindowJoin>(
      C, A, W1_length, W1_slide, TimeDomain::PROCESSING_TIME, "NONE");

  auto joinCAB = std::make_shared<SlidingWindowJoin>(
      joinCA, B, W2_length, W2_slide, TimeDomain::PROCESSING_TIME, "NONE");

  // Create the JoinPlan for CAB
  auto planCAB = std::make_shared<JoinPlan>(joinCAB);

  // Step 6: Compute the reordered JoinPlan (CAB)
  auto resultStreamCAB = planCAB->compute();
  const auto& resultCAB = resultStreamCAB->getTuples();
  long resultSumCAB = evaluator.computeSum(resultCAB);

  // Step 7: Apply LWO for the sliding window setup:
  // Check if W1 and W2 fulfill the conditions for LWO applicability:
  ASSERT_TRUE(W1_slide % W2_slide == 0)
      << "W2's slide must be a divisor of W1's slide for LWO to apply.";

  // Step 8: Compare the results from the ABC and CAB JoinPlans
  ASSERT_EQ(resultSumABC, resultSumCAB)
      << "Total sums do not match between ABC and CAB join orders for Case A4 "
         "(Processing Time).";

  // Optionally, you can compare the actual tuples between the two plans
  std::stringstream errorStream;
  bool resultsEqual =
      evaluator.compareResults(resultABC, resultCAB, errorStream);
  ASSERT_TRUE(resultsEqual) << "Result tuples are not equal between ABC and "
                               "CAB join orders for Case A4.\n"
                            << errorStream.str();
}
