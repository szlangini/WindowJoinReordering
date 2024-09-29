#include <gtest/gtest.h>

#include <memory>

#include "JoinOrderer.h"
#include "JoinPlan.h"
#include "WindowJoinOperator.h"
#include "WindowSpecification.h"

class JoinOrdererTest : public ::testing::Test {
 protected:
  void SetUp() override {}

  JoinOrderer joinOrderer;
};

TEST_F(JoinOrdererTest, TestGeneratePermutations) {
  std::vector<std::string> streams = {"A", "B", "C"};
  std::vector<std::vector<std::string>> permutations;

  joinOrderer.generatePermutations(streams, permutations);
  EXPECT_EQ(permutations.size(), 6);  // 3! = 6 permutations for 3 streams

  std::vector<std::vector<std::string>> expectedPermutations = {
      {"A", "B", "C"}, {"A", "C", "B"}, {"B", "A", "C"},
      {"B", "C", "A"}, {"C", "A", "B"}, {"C", "B", "A"}};
  EXPECT_EQ(permutations, expectedPermutations);
}

TEST_F(JoinOrdererTest, TestDecomposeJoinPair) {
  // Creating a JoinKey with three left streams (representing a more complex
  // join)
  std::unordered_set<std::string> leftStreams = {"A", "B", "C"};
  std::unordered_set<std::string> rightStreams = {"D"};
  JoinKey complexJoinKey(JoinType::SlidingWindowJoin, leftStreams,
                         rightStreams);

  // Decompose the complex join
  std::vector<JoinKey> decomposedPairs =
      joinOrderer.decomposeJoinPair(complexJoinKey);

  // Expect three decomposed pairs: {A, D}, {B, D}, {C, D}
  EXPECT_EQ(decomposedPairs.size(), 3);

  std::unordered_set<std::string> expectedPairsLeft1 = {"A"};
  std::unordered_set<std::string> expectedPairsLeft2 = {"B"};
  std::unordered_set<std::string> expectedPairsLeft3 = {"C"};

  EXPECT_EQ(decomposedPairs[0].leftStreams, expectedPairsLeft1);
  EXPECT_EQ(decomposedPairs[1].leftStreams, expectedPairsLeft2);
  EXPECT_EQ(decomposedPairs[2].leftStreams, expectedPairsLeft3);

  std::unordered_set<std::string> expectedRight = {"D"};
  EXPECT_EQ(decomposedPairs[0].rightStreams, expectedRight);
  EXPECT_EQ(decomposedPairs[1].rightStreams, expectedRight);
  EXPECT_EQ(decomposedPairs[2].rightStreams, expectedRight);
}

TEST_F(JoinOrdererTest, TestCreateUpdatedWindowAssignments) {
  // Mock window assignments (before update)
  std::unordered_map<JoinKey, std::vector<WindowSpecification>>
      windowAssignments;

  // Mock time propagators
  std::unordered_map<WindowSpecification, std::string> timePropagators;

  // Create JoinKey for a SlidingWindowJoin
  std::unordered_set<std::string> leftStreams = {"A", "B"};
  std::unordered_set<std::string> rightStreams = {"C"};
  JoinKey slidingJoinKey(JoinType::SlidingWindowJoin, leftStreams,
                         rightStreams);

  // Create WindowSpecification and assign it
  WindowSpecification slidingWindowSpec =
      WindowSpecification::createSlidingWindowSpecification(1000, 500);
  windowAssignments[slidingJoinKey] = {slidingWindowSpec};

  // Add time propagator for this spec
  timePropagators[slidingWindowSpec] = "A";  // Stream "A" is propagating time

  // Call the function to update window assignments
  joinOrderer.createUpdatedWindowAssignments(windowAssignments,
                                             timePropagators);

  // Expect that the window assignments are updated correctly
  auto updatedWindowSpecs = windowAssignments[slidingJoinKey];

  // Check that the size and contents are correct after update
  EXPECT_EQ(updatedWindowSpecs.size(), 1);
  EXPECT_EQ(updatedWindowSpecs[0].length, 1000);
  EXPECT_EQ(updatedWindowSpecs[0].slide, 500);

  // Verify that the correct time propagator is assigned
  std::string timePropagator = timePropagators.at(slidingWindowSpec);
  EXPECT_EQ(timePropagator, "A");
}
