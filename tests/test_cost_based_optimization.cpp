// This test tries cost-based optimziation for SWJ and IVJ each.
// First, we do unit tests for hardcoded plans, then we do reodering + costing.
#include <gtest/gtest.h>

#include <memory>
#include <unordered_map>

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
