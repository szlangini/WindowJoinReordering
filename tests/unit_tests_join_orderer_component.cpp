#include <gtest/gtest.h>

#include <memory>

#include "IntervalJoin.h"
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

TEST_F(JoinOrdererTest, TestGatherStreamsSingleStream) {
  // Create a mock stream
  auto streamA = std::make_shared<Stream>("StreamA");

  // Map to store the gathered streams
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;

  // Call gatherStreams with the single stream
  joinOrderer.gatherStreams(streamA, streamMap);

  // We should have exactly one stream in the map
  ASSERT_EQ(streamMap.size(), 1);
  EXPECT_EQ(streamMap["StreamA"],
            streamA);  // Check if the stream is stored by name
}

TEST_F(JoinOrdererTest, TestGatherStreamsWithSlidingWindowJoin) {
  // Create mock streams
  auto streamA = std::make_shared<Stream>("StreamA");
  auto streamB = std::make_shared<Stream>("StreamB");

  // Create a SlidingWindowJoin with two streams as children
  auto slidingJoin = std::make_shared<SlidingWindowJoin>(
      streamA, streamB, 1000, 500, TimeDomain::EVENT_TIME, "A");

  // Map to store the gathered streams
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;

  // Call gatherStreams with the sliding window join
  joinOrderer.gatherStreams(slidingJoin, streamMap);

  // We should have exactly two streams in the map
  ASSERT_EQ(streamMap.size(), 2);
  EXPECT_EQ(streamMap["StreamA"],
            streamA);  // Check if StreamA is stored correctly
  EXPECT_EQ(streamMap["StreamB"],
            streamB);  // Check if StreamB is stored correctly
}

TEST_F(JoinOrdererTest, TestGatherStreamsWithIntervalJoin) {
  // Create mock streams
  auto streamA = std::make_shared<Stream>("StreamA");
  auto streamC = std::make_shared<Stream>("StreamC");

  // Create an IntervalJoin with two streams as children
  auto intervalJoin =
      std::make_shared<IntervalJoin>(streamA, streamC, -500, 500, "C");

  // Map to store the gathered streams
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;

  // Call gatherStreams with the interval join
  joinOrderer.gatherStreams(intervalJoin, streamMap);

  // We should have exactly two streams in the map
  ASSERT_EQ(streamMap.size(), 2);
  EXPECT_EQ(streamMap["StreamA"],
            streamA);  // Check if StreamA is stored correctly
  EXPECT_EQ(streamMap["StreamC"],
            streamC);  // Check if StreamC is stored correctly
}

TEST_F(JoinOrdererTest, TestGatherStreamsWithNestedJoins) {
  // Create mock streams
  auto streamA = std::make_shared<Stream>("StreamA");
  auto streamB = std::make_shared<Stream>("StreamB");
  auto streamC = std::make_shared<Stream>("StreamC");

  // Create an IntervalJoin with streamA and streamB
  auto intervalJoin =
      std::make_shared<IntervalJoin>(streamA, streamB, -500, 500, "B");

  // Create a SlidingWindowJoin with intervalJoin and streamC
  auto slidingJoin = std::make_shared<SlidingWindowJoin>(
      intervalJoin, streamC, 1000, 500, TimeDomain::EVENT_TIME, "C");

  // Map to store the gathered streams
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;

  // Call gatherStreams with the sliding window join (nested join tree)
  joinOrderer.gatherStreams(slidingJoin, streamMap);

  // We should have exactly three streams in the map
  ASSERT_EQ(streamMap.size(), 3);
  EXPECT_EQ(streamMap["StreamA"],
            streamA);  // Check if StreamA is stored correctly
  EXPECT_EQ(streamMap["StreamB"],
            streamB);  // Check if StreamB is stored correctly
  EXPECT_EQ(streamMap["StreamC"],
            streamC);  // Check if StreamC is stored correctly
}

// Needs more thought.
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
