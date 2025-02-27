// This test tries cost-based optimization for SWJ and IVJ each.
// First, we do unit tests for hardcoded join-step cost estimation,
// then we do reordering + costing.
#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>

#include "IntervalJoin.h"
#include "JoinOrderer.h"
#include "JoinPlan.h"
#include "SlidingWindowJoin.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "Utils.h"
#include "WindowSpecification.h"

// Test 1: Direct SWJ cost estimation using the helper function.
TEST(CostEstimationTest, SlidingWindowJoinCost) {
  // Setup: effective rates provided explicitly.
  double leftRate = 10.0;
  double rightRate = 20.0;
  // Create a SWJ window specification.
  WindowSpecification swjSpec;
  swjSpec.type = WindowSpecification::WindowType::SLIDING_WINDOW;
  swjSpec.slide = 3;
  swjSpec.length = 7;
  // Expected cost:
  // leftRate * rightRate = 10 * 20 = 200.
  // window factor = (7/1 * 1/3) = 7/3 ≈ 2.33333333333.
  // Total cost = 200 * 2.33333333333 ≈ 466.66666666667.
  double expectedCost = 466.66666666667;
  double cost = JoinOrderer::estimateCostSWJ(swjSpec, leftRate, rightRate, 1);
  EXPECT_NEAR(cost, expectedCost, 1e-9);
}

// Test 2: Direct IVJ cost estimation using the helper function.
TEST(CostEstimationTest, IntervalWindowJoinCost) {
  // Setup: effective rates provided explicitly.
  double leftRate = 10.0;
  double rightRate = 20.0;
  // Create an IVJ window specification.
  WindowSpecification ivjSpec;
  ivjSpec.type = WindowSpecification::WindowType::INTERVAL_WINDOW;
  ivjSpec.lowerBound = 3;
  ivjSpec.upperBound = 7;
  // Expected cost: leftRate * rightRate = 10 * 20 = 200.
  // window factor = (3+7)/1 = 10.
  // Total cost = 200 * 10 = 2000.
  double expectedCost = 2000.0;
  double cost = JoinOrderer::estimateCostIVJ(ivjSpec, leftRate, rightRate, 1);
  EXPECT_NEAR(cost, expectedCost, 1e-9);
}

// Test 3: Reordering and cost ranking for a 3-way SWJ.
TEST(CostEstimationTest, SWJ_Reordering_And_Costing) {
  // Step 1: Create Streams A, B, C; each stream has 5 tuples over a max
  // timestamp of 100. Thus, each stream's rate = 5/100 = 0.05.
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define window settings for Case A3:
  // w1 for join A:B, w2 for join (A:B):C, with a common slide = 5.
  long lengthW1 = 10;  // w1 for A:B
  long lengthW2 = 20;  // w2 for (A:B):C
  long slide = 5;

  // Step 3: Create an initial JoinPlan for A, B, C in Event Time.
  // First join (A:B) uses window w1 and second join ((A:B):C) uses window w2.
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, lengthW1, slide, TimeDomain::EVENT_TIME, "A");
  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, lengthW2, slide, TimeDomain::EVENT_TIME, "A");
  auto initialPlanABC = std::make_shared<JoinPlan>(joinABC);

  // Step 4: Instantiate JoinOrderer and get reordered plans.
  JoinOrderer orderer;
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans =
      orderer.reorder(initialPlanABC);
  ASSERT_GT(reorderedPlans.size(), 0)
      << "No reordering plans generated for 3-way SWJ in ET.";

  // Step 5: Manually compute the expected best cost.
  // Base rates: each stream's rate = 0.05, so product of base rates = 0.05^3 =
  // 0.000125. Join Step 1 (A:B) with w1: cost factor = (length/Δt * Δt/slide) =
  // (10/1 * 1/5) = 2. Cost for join step 1 = 0.05 * 0.05 * 2 = 0.0025 * 2 =
  // 0.005. Join Step 2 ((A:B):C) with w2: effective rate = (A:B) join rate *
  // rate(C) = (0.05*0.05) * 0.05 = 0.000125. Cost factor for step 2 = (20/1 *
  // 1/5) = 4. Cost for join step 2 = 0.000125 * 4 = 0.0005. Total expected cost
  // = 0.005 + 0.0005 = 0.0055.
  double expectedBestCost = 0.0055;

  // Step 6: Evaluate each reordered plan's cost and find the minimum.
  double bestCost = std::numeric_limits<double>::max();
  for (const auto& plan : reorderedPlans) {
    std::cout << "SWJ Plan cost: " << plan->getCost() << std::endl;
    bestCost = std::min(bestCost, plan->getCost());
  }

  // Step 7: Compare the best (lowest) cost with the expected best cost.
  ASSERT_NEAR(bestCost, expectedBestCost, 1e-9)
      << "The best SWJ plan cost (" << bestCost
      << ") does not match the expected cost (" << expectedBestCost << ").";
}

// Test 4: Reordering and cost ranking for a 3-way IVJ.
TEST(CostEstimationTest, IVJ_Reordering_And_Costing) {
  // Step 1: Create Streams A, B, C; each has rate = 5/100 = 0.05.
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define Interval Join window settings: lowerBound = 3, upperBound
  // = 7.
  long lowerBound = 3;
  long upperBound = 7;

  // Step 3: Create an initial JoinPlan for A, B, C using IntervalJoin in Event
  // Time. First join (A:B) uses interval [A.ts-3, A.ts+7] and then join
  // ((A:B):C) uses the same.
  auto joinAB =
      std::make_shared<IntervalJoin>(A, B, lowerBound, upperBound, "A");
  auto joinABC =
      std::make_shared<IntervalJoin>(joinAB, C, lowerBound, upperBound, "A");
  auto initialPlanABC = std::make_shared<JoinPlan>(joinABC);

  // Step 4: Instantiate JoinOrderer and get reordered plans.
  JoinOrderer orderer;
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans =
      orderer.reorder(initialPlanABC);
  ASSERT_GT(reorderedPlans.size(), 0)
      << "No reordering plans generated for 3-way IVJ in ET.";

  // Step 5: Manually compute the expected best cost.
  // Base rates: each stream's rate = 0.05, so product of base rates = 0.05^3 =
  // 0.000125. For each IVJ step, cost factor = (lowerBound+upperBound)/Δt =
  // (3+7)/1 = 10. Join Step 1 cost = 0.05 * 0.05 * 10 = 0.0025 * 10 = 0.025.
  // Join Step 2 cost = (0.05*0.05)*0.05 * 10 = 0.000125 * 10 = 0.00125.
  // Total expected cost = 0.025 + 0.00125 = 0.02625.
  double expectedBestCost = 0.02625;

  // Step 6: Evaluate each reordered plan's cost and find the minimum.
  double bestCost = std::numeric_limits<double>::max();
  for (const auto& plan : reorderedPlans) {
    std::cout << "IVJ Plan cost: " << plan->getCost() << std::endl;
    bestCost = std::min(bestCost, plan->getCost());
  }

  // Step 7: Compare the best (lowest) cost with the expected best cost.
  ASSERT_NEAR(bestCost, expectedBestCost, 1e-9)
      << "The best IVJ plan cost (" << bestCost
      << ") does not match the expected cost (" << expectedBestCost << ").";
}