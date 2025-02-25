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

TEST(CostEstimationTest, IntervalWindowJoinCost) {
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

TEST(CostEstimationTest, IntervalWindowJoinCost2) {
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
