#include <gtest/gtest.h>

#include <memory>
#include <sstream>

#include "IntervalJoin.h"
#include "JoinPlan.h"
#include "ResultEvaluator.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "Utils.h"

#define DEBUG_MODE 1

// Test A: Instantiate ABC join and compute resultSum
TEST(IntervalJoinTest, JoinABC_ET) {
  // Step 1: Create streams
  auto A = createStream("A", 2, linearValueDistribution, 10, 1);
  auto B = createStream("B", 2, linearValueDistribution, 10, 3);
  auto C = createStream("C", 2, linearValueDistribution, 10, 5);

  // Step 2: Define IntervalJoin settings
  long LB = 5;
  long UB = 5;

  // Step 3: Create JoinPlan for ABC using IntervalJoin with Event Time
  // A ⋈ B ⋈ C
  auto joinAB_ABC_ET = std::make_shared<IntervalJoin>(A, B, LB, UB, "A");
  auto resultStreamAB_ET = joinAB_ABC_ET->compute();
  const auto& resultAB = resultStreamAB_ET->getTuples();
#if DEBUG_MODE
  std::cout << "Matches for pairs: \n";
  for (const auto& resultTuple : resultAB) {
    std::cout << resultTuple.toString() << std::endl;
  }
  std::cout << "Matches complete: \n";

#endif

  auto joinABC_ET =
      std::make_shared<IntervalJoin>(joinAB_ABC_ET, C, LB, UB, "A");
  auto initialPlanABC_ET = std::make_shared<JoinPlan>(joinABC_ET);

  // Step 4: Compute Result from JoinPlan ABC
  ResultEvaluator evaluator;
  auto resultStreamABC_ET = initialPlanABC_ET->compute();
  const auto& resultABC_ET = resultStreamABC_ET->getTuples();

#if DEBUG_MODE
  for (const auto& resultTuple : resultABC_ET) {
    std::cout << resultTuple.toString() << std::endl;
  }
#endif

  long resultSumABC_ET = evaluator.computeSum(resultABC_ET);

  // Step 5: Manually calculate the expected sum based on tuple values
  long expectedSumABC_ET = 9;

  // Step 6: Compare the computed sum to the expected sum
  ASSERT_EQ(resultSumABC_ET, expectedSumABC_ET)
      << "ABC Join Sum mismatch. Expected: " << expectedSumABC_ET
      << ", Got: " << resultSumABC_ET;
}

TEST(IntervalJoinTest, Compare_ABC_CAB_ET) {
  // Step 1: Create streams
  auto A = createStream("A", 2, linearValueDistribution, 10, 1);
  auto B = createStream("B", 2, linearValueDistribution, 10, 3);
  auto C = createStream("C", 2, linearValueDistribution, 10, 5);

  // Step 2: Define IntervalJoin settings
  long LB = 5;
  long UB = 5;

  // Step 3: Create JoinPlan for ABC_ET using IntervalJoin with Event Time,
  // propagating "A"
  auto joinAB_ABC_ET = std::make_shared<IntervalJoin>(A, B, LB, UB, "A");
  auto joinABC_ET =
      std::make_shared<IntervalJoin>(joinAB_ABC_ET, C, LB, UB, "A");
  auto initialPlanABC_ET = std::make_shared<JoinPlan>(joinABC_ET);

  // Step 4: Compute Result from JoinPlan ABC_ET
  ResultEvaluator evaluator;
  auto resultStreamABC_ET = initialPlanABC_ET->compute();
  const auto& resultABC_ET = resultStreamABC_ET->getTuples();
  long resultSumABC_ET = evaluator.computeSum(resultABC_ET);

  // Step 5: Create JoinPlan for CAB_ET using IntervalJoin with Event Time,
  // propagating "A"
  auto joinCA_CAB_ET = std::make_shared<IntervalJoin>(C, A, LB, UB, "A");
  auto joinCAB_ET =
      std::make_shared<IntervalJoin>(joinCA_CAB_ET, B, LB, UB, "A");
  auto initialPlanCAB_ET = std::make_shared<JoinPlan>(joinCAB_ET);

  // Step 6: Compute Result from JoinPlan CAB_ET
  auto resultStreamCAB_ET = initialPlanCAB_ET->compute();
  const auto& resultCAB_ET = resultStreamCAB_ET->getTuples();
  long resultSumCAB_ET = evaluator.computeSum(resultCAB_ET);

  // Step 7: Compare the computed sums
  ASSERT_EQ(resultSumABC_ET, resultSumCAB_ET)
      << "ABC Join Sum mismatch. Expected same sum for ABC and CAB. "
      << "ABC Sum: " << resultSumABC_ET << ", CAB Sum: " << resultSumCAB_ET;

  // Step 8: Optionally compare individual tuples to ensure correctness
  std::stringstream errorStream;
  bool resultsEqual =
      evaluator.compareResults(resultABC_ET, resultCAB_ET, errorStream);

  if (!resultsEqual) {
    std::cerr << "Results differ between ABC and CAB join orders:\n"
              << errorStream.str();
  }

  ASSERT_TRUE(resultsEqual)
      << "Result tuples are not equal between ABC and CAB join orders.";
}