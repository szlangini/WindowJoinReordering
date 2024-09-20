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

TEST(IntervalJoinTest, UnequalBoundsJoinABC_ET) {
  // Step 1: Create streams A, B, C
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 200, 2);
  auto C = createStream("C", 5, linearValueDistribution, 300, 3);

#if DEBUG_MODE
  A->printTuples();
  B->printTuples();
  C->printTuples();
#endif

  // Step 2: Define IntervalJoin settings with unequal bounds
  long lowerBound = 3;  // Lower bound is smaller
  long upperBound = 6;  // Upper bound is larger

  // Step 3: Create JoinPlan for ABC using IntervalJoin with Event Time
  // A ⋈ B ⋈ C
  auto joinAB =
      std::make_shared<IntervalJoin>(A, B, lowerBound, upperBound, "A");
  auto joinABC = std::make_shared<IntervalJoin>(
      joinAB, C, lowerBound, upperBound, joinAB->getTimestampPropagator());

  // Step 4: Create a JoinPlan
  auto planABC = std::make_shared<JoinPlan>(joinABC);

  // Step 5: Compute the result of the JoinPlan
  ResultEvaluator evaluator;
  auto resultStreamABC = planABC->compute();
  const auto& resultABC = resultStreamABC->getTuples();

// Step 6: Print results for debug (optional)
#if DEBUG_MODE
  std::cout << "Join results for IntervalJoin with unequal bounds (ET): \n";
  for (const auto& tuple : resultABC) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 7: Calculate result sum and manually compare
  long resultSum = evaluator.computeSum(resultABC);
  long expectedSum = 6;  // Manually calculate or assume expected result sum
                         // based on stream values
  ASSERT_EQ(resultSum, expectedSum)
      << "Result sum mismatch for IntervalJoin with unequal bounds.";
}

TEST(IntervalJoinTest, AB_vs_BA_PropagatingA_With_And_Without_Negated_Bounds) {
  // Step 1: Create streams A and B
  auto A = createStream("A", 10, linearValueDistribution, 100, 1);
  auto B = createStream("B", 10, linearValueDistribution, 200, 2);

#if DEBUG_MODE
  A->printTuples();
  B->printTuples();
#endif

  // Step 2: Define IntervalJoin settings with unequal bounds
  long lowerBound = 0;  // Lower bound is smaller
  long upperBound = 5;  // Upper bound is larger

  // Step 3: Create JoinPlan for AB using IntervalJoin with Event Time
  // A ⋈ B
  auto joinAB =
      std::make_shared<IntervalJoin>(A, B, lowerBound, upperBound, "A");

  // Step 4: Create a JoinPlan for AB
  auto planAB = std::make_shared<JoinPlan>(joinAB);

  // Step 5: Compute the result of the JoinPlan for AB
  ResultEvaluator evaluator;
  auto resultStreamAB = planAB->compute();
  const auto& resultAB = resultStreamAB->getTuples();

  // Step 6: Print results for debug (optional)
#if DEBUG_MODE
  std::cout << "Join results for IntervalJoin AB (ET): \n";
  for (const auto& tuple : resultAB) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 7: Calculate result sum for AB
  long resultSumAB = evaluator.computeSum(resultAB);

  // Step 8: Create JoinPlan for BA using IntervalJoin with Event Time
  // B ⋈ A (A is still the timestamp propagator)
  //   auto joinBA =
  //       std::make_shared<IntervalJoin>(B, A, lowerBound, upperBound, "A");

  //   // Step 9: Create a JoinPlan for BA
  //   auto planBA = std::make_shared<JoinPlan>(joinBA);

  //   // Step 10: Compute the result of the JoinPlan for BA
  //   auto resultStreamBA = planBA->compute();
  //   const auto& resultBA = resultStreamBA->getTuples();

  // // Step 11: Print results for debug (optional)
  // #if DEBUG_MODE
  //   std::cout << "Join results for IntervalJoin BA (ET) propagating A: \n";
  //   for (const auto& tuple : resultBA) {
  //     std::cout << tuple.toString() << std::endl;
  //   }
  // #endif

  //   // Step 12: Calculate result sum for BA
  //   long resultSumBA = evaluator.computeSum(resultBA);

  //   // Step 13: Compare result sums of AB and BA (they should be equal)
  //   ASSERT_EQ(resultSumAB, resultSumBA)
  //       << "Result sums should be equal between AB and BA when propagating
  //       A.";

  // Step 14: Now, we swap and negate the bounds for BA, still propagating A
  long negatedLowerBound = upperBound;  // Negating and swapping
  long negatedUpperBound = lowerBound;

  // Step 15: Create a new BA plan with swapped and negated bounds
  auto joinBA_Negated = std::make_shared<IntervalJoin>(B, A, negatedLowerBound,
                                                       negatedUpperBound, "A");

  // Step 16: Create a JoinPlan for BA with negated bounds
  auto planBA_Negated = std::make_shared<JoinPlan>(joinBA_Negated);

  // Step 17: Compute the result of the JoinPlan for BA with negated bounds
  auto resultStreamBA_Negated = planBA_Negated->compute();
  const auto& resultBA_Negated = resultStreamBA_Negated->getTuples();

  // Step 18: Print results for debug (optional)
#if DEBUG_MODE
  std::cout << "Join results for IntervalJoin BA (ET) with negated bounds: \n";
  for (const auto& tuple : resultBA_Negated) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 19: Calculate result sum for BA with negated bounds
  long resultSumBA_Negated = evaluator.computeSum(resultBA_Negated);

  // Step 20: Compare result sums of AB and BA with negated bounds (expected to
  // be different)
  // ASSERT_NE(resultSumBA, resultSumBA_Negated)
  //     << "Result sums should differ between BA with original and negated "
  //        "bounds.";

  // Step 21: Compare result sums of AB and BA with negated bounds (expected to
  // be equal)
  ASSERT_EQ(resultSumAB, resultSumBA_Negated)
      << "Result sums should not differ between AB and BA with negated bounds.";
}

TEST(IntervalJoinTest,
     UnequalBoundsJoinABC_vs_BAC_ET_With_Swapped_Negated_Bounds) {
  // Step 1: Create streams A, B, C
  auto A = createStream("A", 25, linearValueDistribution, 100, 1);
  auto B = createStream("B", 25, linearValueDistribution, 200, 2);
  auto C = createStream("C", 25, linearValueDistribution, 300, 3);

#if 0 & DEBUG_MODE
  A->printTuples();
  B->printTuples();
  C->printTuples();
#endif

  // Step 2: Define IntervalJoin settings with unequal bounds
  long lowerBound = 0;  // Lower bound is smaller
  long upperBound = 5;  // Upper bound is larger

  // Step 3: Create JoinPlan for ABC using IntervalJoin with Event Time
  // A ⋈ B ⋈ C
  auto joinAB =
      std::make_shared<IntervalJoin>(A, B, lowerBound, upperBound, "A");
  auto joinABC = std::make_shared<IntervalJoin>(
      joinAB, C, lowerBound, upperBound, joinAB->getTimestampPropagator());

  // Step 4: Create a JoinPlan for ABC
  auto planABC = std::make_shared<JoinPlan>(joinABC);

  // Step 5: Compute the result of the JoinPlan for ABC
  ResultEvaluator evaluator;
  auto resultStreamABC = planABC->compute();
  const auto& resultABC = resultStreamABC->getTuples();

  // Step 6: Print results for debug (optional)
#if DEBUG_MODE & 0
  std::cout << "Join results for IntervalJoin ABC (ET): \n";
  for (const auto& tuple : resultABC) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 7: Calculate result sum for ABC
  long resultSumABC = evaluator.computeSum(resultABC);

  // Step 8: Create JoinPlan for BAC using IntervalJoin with Event Time
  // B ⋈ A ⋈ C (A is the timestamp propagator)
  auto joinBA =
      std::make_shared<IntervalJoin>(B, A, lowerBound, upperBound, "A");
  auto joinBAC = std::make_shared<IntervalJoin>(
      joinBA, C, lowerBound, upperBound, joinBA->getTimestampPropagator());

  // Step 9: Create a JoinPlan for BAC
  auto planBAC = std::make_shared<JoinPlan>(joinBAC);

  // Step 10: Compute the result of the JoinPlan for BAC
  auto resultStreamBAC = planBAC->compute();
  const auto& resultBAC = resultStreamBAC->getTuples();

  // Step 11: Print results for debug (optional)
#if DEBUG_MODE & 0
  std::cout << "Join results for IntervalJoin BAC (ET) with A propagated: \n";
  for (const auto& tuple : resultBAC) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 12: Calculate result sum for BAC with A as the propagator
  long resultSumBAC = evaluator.computeSum(resultBAC);

  // Step 13: Compare result sums of ABC and BAC with A propagated, they should
  // be equal
  ASSERT_NE(resultSumABC, resultSumBAC)
      << "Result sums should NOT be equal between ABC and BAC with A as "
         "propagator.";

  // Step 14: Now, we swap and negate the bounds for BAC and change the
  // propagator to B
  long negatedLowerBound = upperBound;  // Negating and swapping
  long negatedUpperBound = lowerBound;

  // Step 15: Create a new BAC plan with swapped and negated bounds and B as the
  // propagator
  auto joinBA_Negated = std::make_shared<IntervalJoin>(B, A, negatedLowerBound,
                                                       negatedUpperBound, "A");
  auto joinBAC_Negated =
      std::make_shared<IntervalJoin>(joinBA_Negated, C, lowerBound, upperBound,
                                     joinBA_Negated->getTimestampPropagator());

  // Step 16: Create a JoinPlan for BAC with negated bounds and B as the
  // propagator
  auto planBAC_Negated = std::make_shared<JoinPlan>(joinBAC_Negated);

#if DEBUG_MODE
  std::cout << planBAC_Negated->toString() << std::endl;
#endif

  // Step 17: Compute the result of the JoinPlan for BAC with negated bounds
  auto resultStreamBAC_Negated = planBAC_Negated->compute();
  const auto& resultBAC_Negated = resultStreamBAC_Negated->getTuples();

// Step 18: Print results for debug (optional)
#if DEBUG_MODE
  std::cout << "Join results for IntervalJoin BAC (ET) with B as propagator "
               "and negated bounds: \n";
  for (const auto& tuple : resultBAC_Negated) {
    std::cout << tuple.toString() << std::endl;
  }
#endif

  // Step 19: Calculate result sum for BAC with negated bounds and B as the
  // propagator
  long resultSumBAC_Negated = evaluator.computeSum(resultBAC_Negated);

  // Step 20: Compare result sums of BAC with A and B as propagators
  ASSERT_NE(resultSumBAC, resultSumBAC_Negated)
      << "Result sums should differ between BAC with A as propagator and "
         "BAC "
         "with B as propagator and negated bounds.";

  // Step 21: Compare result sums of ABC and BAC with negated bounds (expected
  // to be equal)
  ASSERT_EQ(resultSumABC, resultSumBAC_Negated)
      << "Result sums should not differ between ABC and BAC with negated "
         "bounds.";
}
