// tests/test_join_reordering.cpp
// Tests Reordering and result equality/inequality

#include <gtest/gtest.h>

#include "JoinOrderer.h"
#include "JoinPlan.h"
#include "ResultEvaluator.h"
#include "SlidingWindowJoin.h"
#include "Stream.h"
#include "Utils.h"

#define DEBUG_MODE 0

// Value Distributor Func for Automated Content Generation
long linearValueDistribution(int index, int multiplicator) {
  return index + 1;  // Example: values 1, 2, 3, ..., numTuples
}

/*
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 100);  // Random values between 1 and
   100

    // Lambda for generating random values
    auto randomValueDistribution = [&dis, &gen](int index, int multiplicator) {
        return multiplicator * dis(gen);
    };
*/

TEST(StreamTest, AutoGeneratedStream) {
  // Automatically create a stream with 5 tuples, linear values, and max
  // timestamp of 100
  auto streamA = createStream("A", 5, linearValueDistribution, 100, 1);

  // Check that the stream has the correct number of tuples
  const auto& tuples = streamA->getTuples();
  ASSERT_EQ(tuples.size(), 5);

  // Verify the timestamps and values
  ASSERT_EQ(tuples[0].getValues()[0], 1);
  ASSERT_EQ(tuples[0].getTimestamp(), 0);

  ASSERT_EQ(tuples[1].getValues()[0], 2);
  ASSERT_EQ(tuples[1].getTimestamp(),
            25);  // maxTimestamp = 100, step = 100 / (5-1) = 25

  ASSERT_EQ(tuples[4].getValues()[0], 5);
  ASSERT_EQ(tuples[4].getTimestamp(), 100);
}

// Sliding Window Join with equal windows and slide < length (ET Processing
// B.ts)
TEST(JoinReorderingTest, ReorderingValidation_SlidingWJ_Case_A1) {
  ASSERT_GT(1, 0);
  // TODO
}

// Sliding Window Join with equal windows and slide >= length
TEST(JoinReorderingTest, ReorderingValidation_SlidingWJ_Case_A2_ET) {
  // Use the helper function to create streams A, B, C
  // for fuzzy-testing this has to be amended in loop!
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Window settings -- equal size and length
  // this should also be amended in fuzzy-testing
  long length = 10;
  long slide = length;

  // Create an initial JoinPlan for ABC
  // TODO: Make "A" an optional argument so that time propagation can become
  // also PT.
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::EVENT_TIME, "A");
  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, length, slide, TimeDomain::EVENT_TIME, "A");
  auto initialPlan = std::make_shared<JoinPlan>(joinABC);

  // Instantiate the JoinOrderer and reorder the join plan
  JoinOrderer orderer;
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans =
      orderer.reorder(initialPlan);

  // This is the most generic case, same size windows no overlaps => All
  // reordering non-dependent on timestamp propagator should be possible.

  ASSERT_GT(reorderedPlans.size(), 0) << "No reordering plans generated.";

#if DEBUG_MODE
  std::cout << "Found new plans - in total: "
            << std::to_string(reorderedPlans.size()) << std::endl;
#endif

  // Compute and compare results from all reordered plans
  ResultEvaluator evaluator;
  auto referenceStream = initialPlan->compute();
  const auto& referenceResult = referenceStream->getTuples();

  long referenceSum = evaluator.computeSum(referenceResult);

  for (const auto& plan : reorderedPlans) {
#if DEBUG_MODE
    std::cout << plan->toString();
    std::cout << "\n\n";
#endif
    auto resultStream = plan->compute();
    const auto& result = resultStream->getTuples();
    long resultSum = evaluator.computeSum(result);

    // Check if the sum of reordered plans matches the reference sum
    ASSERT_EQ(resultSum, referenceSum)
        << "Reordered plan result sum differs from reference.";

    // Optionally compare tuples
    std::stringstream errorStream;
    bool resultsEqual =
        evaluator.compareResults(referenceResult, result, errorStream);
    ASSERT_TRUE(resultsEqual)
        << "Reordered plan results differ from reference.\n"
        << errorStream.str();
  }
}

TEST(JoinReorderingTest, ReorderingValidation_SlidingWJ_Case_A2_PT) {
  // Step 1: Create Streams A, B, C with sample data
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define Window Settings for Case A2
  // Window settings: equal size and slide (tumbling windows)
  long length = 10;
  long slide = 10;  // Tumbling window since slide == length

  // Step 3: Create Initial JoinPlan for ABC using Processing Time
  // Join A and B with w1, then join the result with C using w2
  auto joinAB_A2_PT = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::PROCESSING_TIME);
  auto joinABC_A2_PT = std::make_shared<SlidingWindowJoin>(
      joinAB_A2_PT, C, length, slide, TimeDomain::PROCESSING_TIME);
  auto initialPlanABC_A2_PT = std::make_shared<JoinPlan>(joinABC_A2_PT);

  // Step 4: Create Initial JoinPlan for CAB using Processing Time
  // Join C and A with w1, then join the result with B using w2
  auto joinCA_A2_PT = std::make_shared<SlidingWindowJoin>(
      C, A, length, slide, TimeDomain::PROCESSING_TIME);
  auto joinCAB_A2_PT = std::make_shared<SlidingWindowJoin>(
      joinCA_A2_PT, B, length, slide, TimeDomain::PROCESSING_TIME);
  auto initialPlanCAB_A2_PT = std::make_shared<JoinPlan>(joinCAB_A2_PT);

  // Step 5: Instantiate JoinOrderer and Reorder the Join Plans
  JoinOrderer orderer;
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans_A2_PT =
      orderer.reorder(initialPlanABC_A2_PT);

  // Ensure that multiple reordered plans are generated (depending on pruning
  // logic)
  ASSERT_GT(reorderedPlans_A2_PT.size(), 0)
      << "No reordering plans generated for Case A2 with PT.";

  // Step 6: Compute Reference Result from Initial JoinPlan ABC
  ResultEvaluator evaluator;
  auto referenceStream_A2_PT = initialPlanABC_A2_PT->compute();
  const auto& referenceResult_A2_PT = referenceStream_A2_PT->getTuples();
  long referenceSum_A2_PT = evaluator.computeSum(referenceResult_A2_PT);

  // Step 7: Validate Reordered Join Plans
  for (const auto& reorderedPlan : reorderedPlans_A2_PT) {
    // Compute the result of the reordered join plan
    auto resultStream = reorderedPlan->compute();
    const auto& resultTuples = resultStream->getTuples();
    long resultSum = evaluator.computeSum(resultTuples);

    // Compare the sum of tuples with the reference sum
    ASSERT_EQ(resultSum, referenceSum_A2_PT)
        << "Result and referenceSum not equal";

    // Optionally, compare the actual tuples for exact match
    std::stringstream errorStream;
    bool resultsEqual = evaluator.compareResults(referenceResult_A2_PT,
                                                 resultTuples, errorStream);
    ASSERT_TRUE(resultsEqual) << "Result tuples differ" << errorStream.str();
  }
}

// Sliding Window Join with unequal windows and slide < length (ET Processing
// B.ts)
TEST(JoinReorderingTest, ReorderingValidation_SlidingWJ_Case_A3) {
  ASSERT_GT(1, 0);
  // TODO
}

// Sliding Window Join with unequal windows and slide >= length (all incl. PT)
TEST(JoinReorderingTest, ReorderingValidation_SlidingWJ_Case_A4) {
  ASSERT_GT(1, 0);
  // TODO
}