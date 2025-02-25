// This test tries cost-based optimziation for SWJ and IVJ each.
// First, we do unit tests for hardcoded plans, then we do reodering + costing.
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

TEST(CostEstimationTest, SlidingWindowJoinCost) {
  // Create two streams; rates are derived as numTuples / maxTimestamp.
  auto streamA =
      createStream("A", 10, linearValueDistribution, 100, 1);  // rate = 0.1
  auto streamB =
      createStream("B", 5, linearValueDistribution, 100, 2);  // rate = 0.05

  // Define window parameters.
  long length = 10;
  long slide = 5;
  auto timeDomain = TimeDomain::PROCESSING_TIME;
  std::string timestampProp = "NONE";

  // Build a SlidingWindowJoin and its JoinPlan.
  auto joinOp = std::make_shared<SlidingWindowJoin>(
      streamA, streamB, length, slide, timeDomain, timestampProp);
  auto joinPlan = std::make_shared<JoinPlan>(joinOp);

  // Create the corresponding WindowSpecification.
  WindowSpecification windowSpec =
      WindowSpecification::createSlidingWindowSpecification(length, slide,
                                                            timestampProp);
  std::vector<WindowSpecification> windows = {windowSpec};

  // Prepare stream map.
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap = {
      {"A", streamA}, {"B", streamB}};

  // Expected cost calculation:
  // streamA rate = 10/100 = 0.1, streamB rate = 5/100 = 0.05 => product =
  // 0.005. windowSizeProduct = (10/1)^2 = 100. slideFactor = (1/5) = 0.2. Total
  // cost = 0.005 * 100 * 0.2 = 0.1.
  double expectedCost = 0.1;

  JoinOrderer orderer;
  double cost = orderer.estimateCost(joinPlan, windows, streamMap);

  ASSERT_NEAR(cost, expectedCost, 1e-9);
}

TEST(CostEstimationTest, SlidingWindowJoinCost2) {
  // Setup: two streams with rates 10 and 20.
  std::vector<double> streamRates = {10.0, 20.0};

  // Create a single IntervalJoin window specification.
  WindowSpecification swjSpec;
  swjSpec.type = WindowSpecification::WindowType::SLIDING_WINDOW;
  swjSpec.slide = 3;
  swjSpec.length = 7;
  std::vector<WindowSpecification> windows = {swjSpec};

  // Expected cost calculation:
  // streamA rate = 10, streamB rate = 20 => product = 200.
  // windowSizeProduct = (7/1)^2 = 49. slideFactor = (1/3) = 0.3333. Total
  // cost = 200 * 49 * 0.3333 = 3266.6666666666665.
  double expectedCost = 3266.6666666666665;
  JoinOrderer orderer;
  double cost = orderer.estimateSWJCost(nullptr, windows, streamRates);
  EXPECT_DOUBLE_EQ(cost, expectedCost);
}

TEST(CostEstimationTest, IntervalWindowJoinCost) {
  // Create two streams; rates derived as numTuples / maxTimestamp.
  auto streamE = createStream("E", 50, linearValueDistribution, 1000,
                              1);  // rate = 50/1000 = 0.05
  auto streamF = createStream("F", 100, linearValueDistribution, 1000,
                              2);  // rate = 100/1000 = 0.1

  // Build an IntervalJoin and its JoinPlan with bounds 4 and 6.
  auto joinOp = std::make_shared<IntervalJoin>(streamE, streamF, 4, 6, "NONE");
  auto joinPlan = std::make_shared<JoinPlan>(joinOp);

  // Create the corresponding Interval WindowSpecification.
  WindowSpecification ivjSpec =
      WindowSpecification::createIntervalWindowSpecification(4, 6, "NONE");
  std::vector<WindowSpecification> windows = {ivjSpec};

  // Prepare stream map.
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap = {
      {"E", streamE}, {"F", streamF}};

  // Expected cost calculation:
  // streamE rate = 50/1000 = 0.05, streamF rate = 100/1000 = 0.1 => product =
  // 0.005. window factor = (4+6)/1 = 10. Total cost = 0.005 * 10 = 0.05.
  double expectedCost = 0.05;

  JoinOrderer orderer;
  double cost = orderer.estimateCost(joinPlan, windows, streamMap);
  ASSERT_NEAR(cost, expectedCost, 1e-9);
}
TEST(CostEstimationTest, IntervalWindowJoinCost2) {
  // Setup: two streams with rates 10 and 20.
  std::vector<double> streamRates = {10.0, 20.0};

  // Create a single IntervalJoin window specification.
  WindowSpecification ivjSpec;
  ivjSpec.type = WindowSpecification::WindowType::INTERVAL_WINDOW;
  ivjSpec.lowerBound = 3;
  ivjSpec.upperBound = 7;
  std::vector<WindowSpecification> windows = {ivjSpec};

  // Expected cost: 10 * 20 * ((3+7)/1) = 2000.
  double expectedCost = 2000.0;
  JoinOrderer orderer;
  double cost = orderer.estimateIVJCost(nullptr, windows, streamRates);
  EXPECT_DOUBLE_EQ(cost, expectedCost);
}

TEST(CostEstimationTest, SWJ_Reordering_And_Costing) {
  // Step 1: Create Streams A, B, C with sample data.
  // Each stream has 5 tuples over a maximum timestamp of 100,
  // so each stream's rate is 5/100 = 0.05.
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define Window Settings for Case A3 (different window lengths)
  long lengthW1 = 10;  // First join (A:B) uses a shorter window (w1)
  long lengthW2 = 20;  // Second join ((A:B):C) uses a longer window (w2)
  long slide = 5;      // Common slide value

  // Step 3: Create an initial JoinPlan for ABC with different windows in Event
  // Time. First join (A:B) is done with window w1 and second join ((A:B):C)
  // with window w2.
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
      << "No reordering plans generated for Case A3 with ET.";

  // Step 5: Manually compute the expected best cost.
  // For each stream: rate = 5/100 = 0.05, so product of rates = 0.05^3 =
  // 0.000125. For window w1: normalized length = 10 → squared = 100. For window
  // w2: normalized length = 20 → squared = 400. Window size product = 100 * 400
  // = 40,000. For slide: each window contributes a factor of (1/5)=0.2; overall
  // slide factor = 0.2 * 0.2 = 0.04. Expected cost = 0.000125 * 40,000 * 0.04 =
  // 0.2.
  double expectedBestCost = 0.2;

  // Step 6: Evaluate each reordered plan's cost and find the minimum.
  double bestCost = std::numeric_limits<double>::max();
  for (const auto& plan : reorderedPlans) {
    std::cout << plan->getCost() << std::endl;
    bestCost = std::min(bestCost, plan->getCost());
  }

  // Step 7: Compare the best (lowest) cost with the expected best cost.
  ASSERT_NEAR(bestCost, expectedBestCost, 1e-9)
      << "The best plan cost (" << bestCost
      << ") does not match the expected cost (" << expectedBestCost << ").";
}

TEST(CostEstimationTest, IVJ_Reordering_And_Costing) {
  // Step 1: Create Streams A, B, C with sample data.
  // Each stream has 5 tuples over a maximum timestamp of 100,
  // so each stream's rate is 5/100 = 0.05.
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define Interval Join Window Settings.
  // For each IntervalJoin, we use lowerBound = 3 and upperBound = 7.
  long lowerBound = 3;
  long upperBound = 7;

  // Step 3: Create an initial JoinPlan for ABC using IntervalJoin in Event
  // Time. First join (A:B) uses the interval [A.ts-3, A.ts+7] and then join
  // ((A:B):C) uses the same bounds.
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
      << "No reordering plans generated for IVJ in Event Time.";

  // Step 5: Manually compute the expected best cost.
  // Each stream's rate = 5/100 = 0.05, so product of rates = 0.05^3 = 0.000125.
  // For each IntervalJoin, the cost factor = (lowerBound+upperBound)/delta_t =
  // (3+7)/1 = 10. Since there are two joins (A:B and then (A:B):C), the window
  // factors multiply: 10 * 10 = 100. Expected cost = 0.000125 * 100 = 0.0125.
  double expectedBestCost = 0.0125;

  // Step 6: Evaluate each reordered plan's cost and find the minimum.
  double bestCost = std::numeric_limits<double>::max();
  for (const auto& plan : reorderedPlans) {
    std::cout << "Plan cost: " << plan->getCost() << std::endl;
    bestCost = std::min(bestCost, plan->getCost());
  }

  // Step 7: Compare the best (lowest) cost with the expected best cost.
  ASSERT_NEAR(bestCost, expectedBestCost, 1e-9)
      << "The best IVJ plan cost (" << bestCost
      << ") does not match the expected cost (" << expectedBestCost << ").";
}
