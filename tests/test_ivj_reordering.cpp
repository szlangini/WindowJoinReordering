#include <gtest/gtest.h>

#include <memory>
#include <sstream>

#include "IntervalJoin.h"
#include "JoinOrderer.h"
#include "JoinPlan.h"
#include "ResultEvaluator.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "Utils.h"

#define DEBUG_MODE 0

// IntervalJoin with equal upper and lower bounds
TEST(JoinReorderingTest, ReorderingValidation_IntervalJoin_Case_ET) {
  // Use the helper function to create streams A, B, C
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Define IntervalJoin settings -- equal upper and lower bounds
  long lowerBound = 5;
  long upperBound = 5;

  // Create an initial JoinPlan for ABC (A ⋈ B ⋈ C) using IntervalJoin
  auto joinAB =
      std::make_shared<IntervalJoin>(A, B, lowerBound, upperBound, "A");
  auto joinABC =
      std::make_shared<IntervalJoin>(joinAB, C, lowerBound, upperBound, "A");
  auto initialPlan = std::make_shared<JoinPlan>(joinABC);

  // Instantiate the JoinOrderer and reorder the join plan
  JoinOrderer orderer;
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans =
      orderer.reorder(initialPlan);

  // Check that reordering plans are generated
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
