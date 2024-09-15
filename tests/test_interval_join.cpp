// tests/test_interval_join.cpp

#include <gtest/gtest.h>

#include <memory>
#include <sstream>

#include "IntervalJoin.h"
#include "JoinPlan.h"
#include "ResultEvaluator.h"
#include "Stream.h"
#include "TimeDomain.h"

// Test A: Instantiate ABC join and compute resultSum
TEST(IntervalJoinTest, JoinABC_ET) {
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

  // Step 2: Define IntervalJoin settings
  long LB = 5;
  long UB = 5;

  // Step 3: Create JoinPlan for ABC using IntervalJoin with Event Time
  // A ⋈ B ⋈ C
  auto joinAB_ABC_ET = std::make_shared<IntervalJoin>(A, B, LB, UB);
  auto joinABC_ET = std::make_shared<IntervalJoin>(joinAB_ABC_ET, C, LB, UB);
  auto initialPlanABC_ET = std::make_shared<JoinPlan>(joinABC_ET);

  // Step 4: Compute Result from JoinPlan ABC
  ResultEvaluator evaluator;
  auto resultStreamABC_ET = initialPlanABC_ET->compute();
  const auto& resultABC_ET = resultStreamABC_ET->getTuples();
  long resultSumABC_ET = evaluator.computeSum(resultABC_ET);

  // TODO: Add proper computation
  long expectedSumABC_ET = 53;

  // Step 6: Compare the computed sum to the expected sum
  ASSERT_EQ(resultSumABC_ET, expectedSumABC_ET)
      << "ABC Join Sum mismatch. Expected: " << expectedSumABC_ET
      << ", Got: " << resultSumABC_ET;
}