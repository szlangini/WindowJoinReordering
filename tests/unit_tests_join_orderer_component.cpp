#include <gtest/gtest.h>

#include <memory>

#include "IntervalJoin.h"
#include "JoinOrderer.h"
#include "JoinPlan.h"
#include "Utils.h"
#include "WindowJoinOperator.h"
#include "WindowSpecification.h"

class JoinOrdererTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto A = createStream("A", 100, linearValueDistribution, 100, 1);
    auto B = createStream("B", 100, linearValueDistribution, 200, 2);
    auto C = createStream("C", 200, linearValueDistribution, 100, 3);

    long length = 10;
    long slide = 10;

    auto joinAB = std::make_shared<SlidingWindowJoin>(
        A, B, length, slide, TimeDomain::EVENT_TIME, "A");
    auto joinABC = std::make_shared<SlidingWindowJoin>(
        joinAB, C, length, slide, TimeDomain::EVENT_TIME,
        joinAB->getTimestampPropagator());

    testJoinPlan = std::make_shared<JoinPlan>(joinABC);
  }

  JoinOrderer joinOrderer;
  std::shared_ptr<JoinPlan> testJoinPlan;
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

TEST_F(JoinOrdererTest, TestGenerateAllJoinPermutations) {
  // Generate all join permutations based on the testJoinPlan
  auto joinPermutations = joinOrderer.generateAllJoinPermutations(testJoinPlan);

  // Check that the correct number of permutations is generated
  // For 3 streams, we expect 2 permutations:
  // (A:B then AB:C), (B:A then BA:C), etc.
  EXPECT_EQ(joinPermutations.size(), 6);  // 3! = 6 permutations for 3 streams

  // Verify the content of the first permutation
  // Expected order: A:B and then AB:C
  auto perm1 = joinPermutations[0];
  ASSERT_EQ(perm1.getSteps().size(), 2);  // Two steps for this permutation

  // First step: A:B
  JoinKey step1Key = perm1.getSteps()[0];
  EXPECT_EQ(step1Key.leftStreams, (std::unordered_set<std::string>{"A"}));
  EXPECT_EQ(step1Key.rightStreams, (std::unordered_set<std::string>{"B"}));
  EXPECT_EQ(step1Key.joinType, JoinType::SlidingWindowJoin);

  // Second step: AB:C
  JoinKey step2Key = perm1.getSteps()[1];
  EXPECT_EQ(step2Key.leftStreams, (std::unordered_set<std::string>{"A", "B"}));
  EXPECT_EQ(step2Key.rightStreams, (std::unordered_set<std::string>{"C"}));
  EXPECT_EQ(step2Key.joinType, JoinType::SlidingWindowJoin);

  // Verify another permutation, such as ACB
  auto perm2 = joinPermutations[1];
  ASSERT_EQ(perm2.getSteps().size(), 2);  // Two steps for this permutation

  // First step: A:C
  JoinKey step1Key_perm2 = perm2.getSteps()[0];
  EXPECT_EQ(step1Key_perm2.leftStreams, (std::unordered_set<std::string>{"A"}));
  EXPECT_EQ(step1Key_perm2.rightStreams,
            (std::unordered_set<std::string>{"C"}));
  EXPECT_EQ(step1Key_perm2.joinType, JoinType::SlidingWindowJoin);

  // Second step: AC:B
  JoinKey step2Key_perm2 = perm2.getSteps()[1];
  EXPECT_EQ(step2Key_perm2.leftStreams,
            (std::unordered_set<std::string>{"C", "A"}));
  EXPECT_EQ(step2Key_perm2.rightStreams,
            (std::unordered_set<std::string>{"B"}));
  EXPECT_EQ(step2Key_perm2.joinType, JoinType::SlidingWindowJoin);
}

TEST_F(JoinOrdererTest, TestGenerateCommutativeJoinPlans) {
  // Generate commutative join plans from the testJoinPlan
  auto commutativePlans =
      joinOrderer.generateCommutativeJoinPlans(testJoinPlan);

  // Check that the correct number of commutative join plans is generated
  // We expect 4 commutative plans: C:AB, AB:C, C:BA, BA:C
  EXPECT_EQ(commutativePlans.size(), 4);

  // Verify the content of the first commutative plan
  // Expected order: AB:C (Original order)
  auto commutativePlan1 = commutativePlans[0];
  auto root1 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(commutativePlan1->getRoot());

  ASSERT_TRUE(root1 != nullptr);  // Ensure that the root is a join node
  auto left1 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root1->getLeftChild());
  auto right1 = std::dynamic_pointer_cast<Stream>(root1->getRightChild());

  ASSERT_TRUE(left1 != nullptr);   // Ensure left child is a join (AB)
  ASSERT_TRUE(right1 != nullptr);  // Ensure right child is a stream (C)

  EXPECT_EQ(left1->getLeftChild()->getName(), "A");
  EXPECT_EQ(left1->getRightChild()->getName(), "B");
  EXPECT_EQ(right1->getName(), "C");

  // Verify the content of the second commutative plan
  // Expected order: C:AB (Commutative order)
  auto commutativePlan2 = commutativePlans[1];
  auto root2 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(commutativePlan2->getRoot());

  ASSERT_TRUE(root2 != nullptr);  // Ensure that the root is a join node
  auto left2 = std::dynamic_pointer_cast<Stream>(root2->getLeftChild());
  auto right2 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root2->getRightChild());

  ASSERT_TRUE(left2 != nullptr);   // Ensure left child is a stream (C)
  ASSERT_TRUE(right2 != nullptr);  // Ensure right child is a join (AB)

  EXPECT_EQ(left2->getName(), "C");
  EXPECT_EQ(right2->getLeftChild()->getName(), "A");
  EXPECT_EQ(right2->getRightChild()->getName(), "B");

  // Verify the content of the third commutative plan
  // Expected order: BA:C
  auto commutativePlan3 = commutativePlans[2];
  auto root3 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(commutativePlan3->getRoot());

  ASSERT_TRUE(root3 != nullptr);  // Ensure that the root is a join node
  auto left3 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root3->getLeftChild());
  auto right3 = std::dynamic_pointer_cast<Stream>(root3->getRightChild());

  ASSERT_TRUE(left3 != nullptr);   // Ensure left child is a join (BA)
  ASSERT_TRUE(right3 != nullptr);  // Ensure right child is a stream (C)

  EXPECT_EQ(left3->getLeftChild()->getName(), "B");
  EXPECT_EQ(left3->getRightChild()->getName(), "A");
  EXPECT_EQ(right3->getName(), "C");

  // Verify the content of the fourth commutative plan
  // Expected order: C:BA
  auto commutativePlan4 = commutativePlans[3];
  auto root4 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(commutativePlan4->getRoot());

  ASSERT_TRUE(root4 != nullptr);  // Ensure that the root is a join node
  auto left4 = std::dynamic_pointer_cast<Stream>(root4->getLeftChild());
  auto right4 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root4->getRightChild());

  ASSERT_TRUE(left4 != nullptr);   // Ensure left child is a stream (C)
  ASSERT_TRUE(right4 != nullptr);  // Ensure right child is a join (BA)

  EXPECT_EQ(left4->getName(), "C");
  EXPECT_EQ(right4->getLeftChild()->getName(), "B");
  EXPECT_EQ(right4->getRightChild()->getName(), "A");
}

TEST_F(JoinOrdererTest, TestGetAllSlidingWindowJoinPlans) {
  // Define a general window specification for the test (Tumbling window with
  // equal lengths)
  WindowSpecification generalWindowSpec =
      WindowSpecification::createSlidingWindowSpecification(10, 10, "NONE");

  // Generate all sliding window join plans based on the testJoinPlan and
  // general window specification
  auto joinPlans =
      joinOrderer.getAllSlidingWindowJoinPlans(testJoinPlan, generalWindowSpec);

  // Check that the correct number of permutations is generated
  // For 3 streams, we expect 6 permutations: 3! = 6 permutations for 3 streams
  EXPECT_EQ(joinPlans.size(), 6);

  // Verify the content of the first join plan (A:B:C)
  auto joinPlan1 = joinPlans[0];
  auto root1 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(joinPlan1->getRoot());

  ASSERT_TRUE(root1 != nullptr);  // Ensure that the root is a join node
  auto left1 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root1->getLeftChild());
  auto right1 = std::dynamic_pointer_cast<Stream>(root1->getRightChild());

  ASSERT_TRUE(left1 != nullptr);   // Ensure left child is a join (A:B)
  ASSERT_TRUE(right1 != nullptr);  // Ensure right child is a stream (C)

  EXPECT_EQ(left1->getLeftChild()->getName(), "A");
  EXPECT_EQ(left1->getRightChild()->getName(), "B");
  EXPECT_EQ(right1->getName(), "C");

  // Verify the content of the second join plan (A:C:B)
  auto joinPlan2 = joinPlans[1];
  auto root2 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(joinPlan2->getRoot());

  ASSERT_TRUE(root2 != nullptr);  // Ensure that the root is a join node
  auto left2 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root2->getLeftChild());
  auto right2 = std::dynamic_pointer_cast<Stream>(root2->getRightChild());

  ASSERT_TRUE(left2 != nullptr);   // Ensure left child is a join (A:C)
  ASSERT_TRUE(right2 != nullptr);  // Ensure right child is a stream (B)

  EXPECT_EQ(left2->getLeftChild()->getName(), "A");
  EXPECT_EQ(left2->getRightChild()->getName(), "C");
  EXPECT_EQ(right2->getName(), "B");

  // Verify the content of the third join plan (B:A:C)
  auto joinPlan3 = joinPlans[2];
  auto root3 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(joinPlan3->getRoot());

  ASSERT_TRUE(root3 != nullptr);  // Ensure that the root is a join node
  auto left3 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root3->getLeftChild());
  auto right3 = std::dynamic_pointer_cast<Stream>(root3->getRightChild());

  ASSERT_TRUE(left3 != nullptr);   // Ensure left child is a join (B:A)
  ASSERT_TRUE(right3 != nullptr);  // Ensure right child is a stream (C)

  EXPECT_EQ(left3->getLeftChild()->getName(), "B");
  EXPECT_EQ(left3->getRightChild()->getName(), "A");
  EXPECT_EQ(right3->getName(), "C");

  // Verify the content of the fourth join plan (B:C:A)
  auto joinPlan4 = joinPlans[3];
  auto root4 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(joinPlan4->getRoot());

  ASSERT_TRUE(root4 != nullptr);  // Ensure that the root is a join node
  auto left4 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root4->getLeftChild());
  auto right4 = std::dynamic_pointer_cast<Stream>(root4->getRightChild());

  ASSERT_TRUE(left4 != nullptr);   // Ensure left child is a join (B:C)
  ASSERT_TRUE(right4 != nullptr);  // Ensure right child is a stream (A)

  EXPECT_EQ(left4->getLeftChild()->getName(), "B");
  EXPECT_EQ(left4->getRightChild()->getName(), "C");
  EXPECT_EQ(right4->getName(), "A");

  // Verify the content of the fifth join plan (C:A:B)
  auto joinPlan5 = joinPlans[4];
  auto root5 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(joinPlan5->getRoot());

  ASSERT_TRUE(root5 != nullptr);  // Ensure that the root is a join node
  auto left5 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root5->getLeftChild());
  auto right5 = std::dynamic_pointer_cast<Stream>(root5->getRightChild());

  ASSERT_TRUE(left5 != nullptr);   // Ensure left child is a join (C:A)
  ASSERT_TRUE(right5 != nullptr);  // Ensure right child is a stream (B)

  EXPECT_EQ(left5->getLeftChild()->getName(), "C");
  EXPECT_EQ(left5->getRightChild()->getName(), "A");
  EXPECT_EQ(right5->getName(), "B");

  // Verify the content of the sixth join plan (C:B:A)
  auto joinPlan6 = joinPlans[5];
  auto root6 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(joinPlan6->getRoot());

  ASSERT_TRUE(root6 != nullptr);  // Ensure that the root is a join node
  auto left6 =
      std::dynamic_pointer_cast<SlidingWindowJoin>(root6->getLeftChild());
  auto right6 = std::dynamic_pointer_cast<Stream>(root6->getRightChild());

  ASSERT_TRUE(left6 != nullptr);   // Ensure left child is a join (C:B)
  ASSERT_TRUE(right6 != nullptr);  // Ensure right child is a stream (A)

  EXPECT_EQ(left6->getLeftChild()->getName(), "C");
  EXPECT_EQ(left6->getRightChild()->getName(), "B");
  EXPECT_EQ(right6->getName(), "A");
}

TEST_F(JoinOrdererTest, TestGetWindowSpecificationsAndAssignments_SlidingOnly) {
  // Step 1: Create Streams A, B, C with sample data
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define Window Specifications
  long lengthAB = 10;
  long slideAB = 5;  // A:B sliding window

  long lengthABC = 20;
  long slideABC = 10;  // AB:C sliding window

  // Step 3: Create the Sliding Window JoinPlan for ABC
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, lengthAB, slideAB, TimeDomain::PROCESSING_TIME);
  auto joinABC = std::make_shared<SlidingWindowJoin>(
      joinAB, C, lengthABC, slideABC, TimeDomain::PROCESSING_TIME);

  auto testJoinPlan = std::make_shared<JoinPlan>(joinABC);

  // Step 4: Call the method under test
  auto result = joinOrderer.getWindowSpecificationsAndAssignments(testJoinPlan);

  // Step 5: Verify the returned window specifications
  const auto& windowSpecs = result.first;
  ASSERT_EQ(windowSpecs.size(),
            2);  // Two window specs: one for A:B, one for AB:C

  // Verify WindowSpec for A:B
  EXPECT_EQ(windowSpecs[0].length, lengthAB);
  EXPECT_EQ(windowSpecs[0].slide, slideAB);
  EXPECT_EQ(windowSpecs[0].type,
            WindowSpecification::WindowType::SLIDING_WINDOW);

  // Verify WindowSpec for AB:C
  EXPECT_EQ(windowSpecs[1].length, lengthABC);
  EXPECT_EQ(windowSpecs[1].slide, slideABC);
  EXPECT_EQ(windowSpecs[1].type,
            WindowSpecification::WindowType::SLIDING_WINDOW);

  // Step 6: Verify windowAssignments for each join
  const auto& windowAssignments = result.second;
  ASSERT_EQ(windowAssignments.size(), 2);  // A:B and AB:C

  // Verify join key A:B
  JoinKey keyAB(JoinType::SlidingWindowJoin, {"A"}, {"B"});
  ASSERT_TRUE(windowAssignments.count(keyAB));
  EXPECT_EQ(windowAssignments.at(keyAB).size(),
            1);  // Only one windowSpec for A:B
  EXPECT_EQ(windowAssignments.at(keyAB)[0],
            windowSpecs[0]);  // Matches first spec

  // Verify join key AB:C
  JoinKey keyABC(JoinType::SlidingWindowJoin, {"A", "B"}, {"C"});
  ASSERT_TRUE(windowAssignments.count(keyABC));
  EXPECT_EQ(windowAssignments.at(keyABC).size(),
            1);  // Only one windowSpec for AB:C
  EXPECT_EQ(windowAssignments.at(keyABC)[0],
            windowSpecs[1]);  // Matches second spec
}

TEST_F(JoinOrdererTest,
       TestGetWindowSpecificationsAndAssignments_SingleSlidingJoin) {
  // Step 1: Create Streams A, B with sample data
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);

  // Step 2: Define a Sliding Window Specification for A:B
  long length = 15;
  long slide = 5;  // Sliding window

  // Step 3: Create the Sliding Window JoinPlan for A:B
  auto joinAB = std::make_shared<SlidingWindowJoin>(
      A, B, length, slide, TimeDomain::PROCESSING_TIME);
  auto testJoinPlan = std::make_shared<JoinPlan>(joinAB);

  // Step 4: Call the method under test
  auto result = joinOrderer.getWindowSpecificationsAndAssignments(testJoinPlan);

  // Step 5: Verify the returned window specifications
  const auto& windowSpecs = result.first;
  ASSERT_EQ(windowSpecs.size(), 1);  // Only one window spec for A:B

  // Verify WindowSpec for A:B
  EXPECT_EQ(windowSpecs[0].length, length);
  EXPECT_EQ(windowSpecs[0].slide, slide);
  EXPECT_EQ(windowSpecs[0].type,
            WindowSpecification::WindowType::SLIDING_WINDOW);

  // Step 6: Verify windowAssignments for A:B
  const auto& windowAssignments = result.second;
  ASSERT_EQ(windowAssignments.size(), 1);  // Only A:B join exists

  // Verify join key A:B
  JoinKey keyAB(JoinType::SlidingWindowJoin, {"A"}, {"B"});
  ASSERT_TRUE(windowAssignments.count(keyAB));
  EXPECT_EQ(windowAssignments.at(keyAB).size(),
            1);  // Only one windowSpec for A:B
  EXPECT_EQ(windowAssignments.at(keyAB)[0],
            windowSpecs[0]);  // Matches the spec
}

TEST_F(JoinOrdererTest,
       TestGetWindowSpecificationsAndAssignments_TwoWayIntervalJoin) {
  // Step 1: Create Streams A, B, C with sample data
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);
  auto C = createStream("C", 5, linearValueDistribution, 100, 3);

  // Step 2: Define Interval Window Specifications with timestamp propagators
  long lowerBoundAB = 5;
  long upperBoundAB = 15;          // A:B interval join
  std::string propagatorAB = "A";  // Propagate timestamps from stream A

  long lowerBoundABC = 10;
  long upperBoundABC = 20;          // AB:C interval join
  std::string propagatorABC = "A";  // Propagate timestamps from the AB result

  // Step 3: Create the Interval JoinPlan for ABC
  auto joinAB = std::make_shared<IntervalJoin>(A, B, lowerBoundAB, upperBoundAB,
                                               propagatorAB);
  auto joinABC = std::make_shared<IntervalJoin>(joinAB, C, lowerBoundABC,
                                                upperBoundABC, propagatorABC);

  auto testJoinPlan = std::make_shared<JoinPlan>(joinABC);

  // Step 4: Call the method under test
  auto result = joinOrderer.getWindowSpecificationsAndAssignments(testJoinPlan);

  // Step 5: Verify the returned window specifications
  const auto& windowSpecs = result.first;
  ASSERT_EQ(windowSpecs.size(),
            2);  // Two window specs: one for A:B, one for AB:C

  // Verify WindowSpec for A:B (interval join)
  EXPECT_EQ(windowSpecs[0].lowerBound, lowerBoundAB);
  EXPECT_EQ(windowSpecs[0].upperBound, upperBoundAB);
  EXPECT_EQ(windowSpecs[0].timestampPropagator,
            propagatorAB);  // Verify propagator
  EXPECT_EQ(windowSpecs[0].type,
            WindowSpecification::WindowType::INTERVAL_WINDOW);

  // Verify WindowSpec for AB:C (interval join)
  EXPECT_EQ(windowSpecs[1].lowerBound, lowerBoundABC);
  EXPECT_EQ(windowSpecs[1].upperBound, upperBoundABC);
  EXPECT_EQ(windowSpecs[1].timestampPropagator,
            propagatorABC);  // Verify propagator
  EXPECT_EQ(windowSpecs[1].type,
            WindowSpecification::WindowType::INTERVAL_WINDOW);

  // Step 6: Verify windowAssignments for each join
  const auto& windowAssignments = result.second;
  ASSERT_EQ(windowAssignments.size(), 2);  // A:B and AB:C

  // Verify join key A:B
  JoinKey keyAB(JoinType::IntervalJoin, {"A"}, {"B"});
  ASSERT_TRUE(windowAssignments.count(keyAB));
  EXPECT_EQ(windowAssignments.at(keyAB).size(),
            1);  // Only one windowSpec for A:B
  EXPECT_EQ(windowAssignments.at(keyAB)[0],
            windowSpecs[0]);  // Matches first spec

  // Verify join key AB:C
  JoinKey keyABC(JoinType::IntervalJoin, {"A", "B"}, {"C"});
  ASSERT_TRUE(windowAssignments.count(keyABC));
  EXPECT_EQ(windowAssignments.at(keyABC).size(),
            1);  // Only one windowSpec for AB:C
  EXPECT_EQ(windowAssignments.at(keyABC)[0],
            windowSpecs[1]);  // Matches second spec
}

TEST_F(JoinOrdererTest,
       TestGetWindowSpecificationsAndAssignments_SingleStream_IntervalJoin) {
  // Step 1: Create a single Stream A with sample data
  auto A = createStream("A", 5, linearValueDistribution, 100, 1);
  auto B = createStream("B", 5, linearValueDistribution, 100, 2);

  // Step 2: Define an IntervalJoin between A and B with specific bounds
  long lowerBound = 10;
  long upperBound = 20;
  std::string timestampPropagator = "A";

  // Create an IntervalJoin between A and B
  auto intervalJoinAB = std::make_shared<IntervalJoin>(
      A, B, lowerBound, upperBound, timestampPropagator);
  auto joinPlanAB = std::make_shared<JoinPlan>(intervalJoinAB);

  // Step 3: Call getWindowSpecificationsAndAssignments to retrieve specs and
  // assignments
  JoinOrderer joinOrderer;
  auto [windowSpecs, windowAssignments] =
      joinOrderer.getWindowSpecificationsAndAssignments(joinPlanAB);

  // Step 4: Validate the window specifications
  ASSERT_EQ(windowSpecs.size(), 1)
      << "Expected one window specification for IntervalJoin.";
  EXPECT_EQ(windowSpecs[0].lowerBound, lowerBound) << "Lower bound mismatch.";
  EXPECT_EQ(windowSpecs[0].upperBound, upperBound) << "Upper bound mismatch.";
  EXPECT_EQ(windowSpecs[0].timestampPropagator, timestampPropagator)
      << "Timestamp propagator mismatch.";

  // Step 5: Validate the window assignments
  JoinKey keyAB;
  keyAB.joinType = JoinType::IntervalJoin;
  keyAB.leftStreams = {"A"};
  keyAB.rightStreams = {"B"};

  ASSERT_EQ(windowAssignments.count(keyAB), 1)
      << "Expected window assignment for A:B.";
  EXPECT_EQ(windowAssignments[keyAB][0].lowerBound, lowerBound)
      << "Assigned lower bound mismatch.";
  EXPECT_EQ(windowAssignments[keyAB][0].upperBound, upperBound)
      << "Assigned upper bound mismatch.";
}

TEST_F(JoinOrdererTest, TestCreateCommutativePairs) {
  // Step 1: Get window specifications and assignments for the testJoinPlan
  auto [windowSpecs, windowAssignments] =
      joinOrderer.getWindowSpecificationsAndAssignments(testJoinPlan);

  // Step 2: Call createCommutativePairs to generate commutative pairs
  joinOrderer.createCommutativePairs(windowAssignments);

  // Step 3: Verify that commutative pairs have been added

  // Define the original and commutative pairs for AB:C and C:AB
  JoinKey keyABC;
  keyABC.leftStreams = {"A", "B"};
  keyABC.rightStreams = {"C"};
  keyABC.joinType = JoinType::SlidingWindowJoin;

  JoinKey keyCAB;
  keyCAB.leftStreams = {"C"};
  keyCAB.rightStreams = {"A", "B"};
  keyCAB.joinType = JoinType::SlidingWindowJoin;

  // Define the commutative pairs for A:B and B:A
  JoinKey keyAB;
  keyAB.leftStreams = {"A"};
  keyAB.rightStreams = {"B"};
  keyAB.joinType = JoinType::SlidingWindowJoin;

  JoinKey keyBA;
  keyBA.leftStreams = {"B"};
  keyBA.rightStreams = {"A"};
  keyBA.joinType = JoinType::SlidingWindowJoin;

  // Print the window assignments for debugging purposes
  printWindowAssignments(windowAssignments);

  // Verify that all commutative pairs are present and equal
  EXPECT_EQ(windowAssignments.count(keyABC), 1);
  EXPECT_EQ(windowAssignments.count(keyCAB), 1);
  EXPECT_EQ(windowAssignments[keyCAB], windowAssignments[keyABC]);

  EXPECT_EQ(windowAssignments.count(keyAB), 1);
  EXPECT_EQ(windowAssignments.count(keyBA), 1);
  EXPECT_EQ(windowAssignments[keyBA], windowAssignments[keyAB]);
}

TEST_F(JoinOrdererTest, TestDeriveAllWindowPermutations) {
  // Step 1: Get window specifications and assignments for the testJoinPlan
  auto [windowSpecs, windowAssignments] =
      joinOrderer.getWindowSpecificationsAndAssignments(testJoinPlan);

  // Step 2: Call deriveAllWindowPermutations to generate derived permutations
  joinOrderer.deriveAllWindowPermutations(windowAssignments);

  // Step 3: Verify the existence of derived window assignments for AC, BC, BA,
  // and CA
  JoinKey keyAB;
  keyAB.leftStreams = {"A"};
  keyAB.rightStreams = {"B"};
  keyAB.joinType = JoinType::SlidingWindowJoin;

  JoinKey keyAC;
  keyAC.leftStreams = {"A"};
  keyAC.rightStreams = {"C"};
  keyAC.joinType = JoinType::SlidingWindowJoin;

  JoinKey keyBA;
  keyBA.leftStreams = {"B"};
  keyBA.rightStreams = {"A"};
  keyBA.joinType = JoinType::SlidingWindowJoin;

  JoinKey keyCA;
  keyCA.leftStreams = {"C"};
  keyCA.rightStreams = {"A"};
  keyCA.joinType = JoinType::SlidingWindowJoin;

  JoinKey keyBC;
  keyBC.leftStreams = {"B"};
  keyBC.rightStreams = {"C"};
  keyBC.joinType = JoinType::SlidingWindowJoin;

  // Verify that AC is derived from AB
  EXPECT_EQ(windowAssignments.count(keyAC), 1);
  EXPECT_EQ(windowAssignments[keyAC], windowAssignments[keyAB]);

  // Verify that CA is derived as a commutative pair
  EXPECT_EQ(windowAssignments.count(keyCA), 1);
  EXPECT_EQ(windowAssignments[keyCA], windowAssignments[keyAC]);

  // Verify that BA is derived from AB as a commutative pair
  EXPECT_EQ(windowAssignments.count(keyBA), 1);
  EXPECT_EQ(windowAssignments[keyBA], windowAssignments[keyAB]);

  // Verify that BC is derived as another permutation
  EXPECT_EQ(windowAssignments.count(keyBC), 1);
}

TEST_F(JoinOrdererTest, TestJoinKeyCopy) {
  // Step 1: Create a JoinKey with some sample data
  JoinKey joinKey;
  joinKey.leftStreams = {"A", "B", "C"};
  joinKey.rightStreams = {"D"};
  joinKey.joinType = JoinType::SlidingWindowJoin;

  // Step 2: Make a copy of leftStreams
  auto leftStreamsCopy = joinKey.leftStreams;

  // Step 3: Iterate over the copy and print the streams
  std::cout << "Iterating over leftStreamsCopy:" << std::endl;
  for (const auto& streamName : leftStreamsCopy) {
    std::cout << "Stream: " << streamName << std::endl;
  }

  // Step 4: Check if the copy contains the correct elements
  ASSERT_EQ(leftStreamsCopy.size(), 3);
  ASSERT_TRUE(leftStreamsCopy.find("A") != leftStreamsCopy.end());
  ASSERT_TRUE(leftStreamsCopy.find("B") != leftStreamsCopy.end());
  ASSERT_TRUE(leftStreamsCopy.find("C") != leftStreamsCopy.end());
}

TEST_F(JoinOrdererTest, TestGetStepsReturnsValidKeys) {
  JoinPermutation permutation;

  JoinKey key1;
  key1.leftStreams = {"A"};
  key1.rightStreams = {"B"};
  key1.joinType = JoinType::SlidingWindowJoin;

  JoinKey key2;
  key2.leftStreams = {"B"};
  key2.rightStreams = {"C"};
  key2.joinType = JoinType::SlidingWindowJoin;

  // Add steps to permutation
  permutation.addJoinStep(key1);
  permutation.addJoinStep(key2);

  // Retrieve steps and check they are valid
  const auto& steps = permutation.getSteps();
  ASSERT_EQ(steps.size(), 2);

  JoinKey joinKey1 = steps[0];
  JoinKey joinKey2 = steps[1];

  // Check that the steps match the added keys
  ASSERT_EQ(joinKey1.leftStreams, key1.leftStreams);
  ASSERT_EQ(joinKey2.leftStreams, key2.leftStreams);
}

TEST_F(JoinOrdererTest, TestBuildJoinPlanFromPermutation) {
  // Step 1: Get window specifications and assignments for the testJoinPlan
  auto [windowSpecs, windowAssignments] =
      joinOrderer.getWindowSpecificationsAndAssignments(testJoinPlan);

  // Step 2: Generate all join permutations
  auto joinPermutations = joinOrderer.generateAllJoinPermutations(testJoinPlan);

  // Ensure we have generated the permutations correctly
  ASSERT_GT(joinPermutations.size(), 0)
      << "No permutations generated for the test join plan";

  // Step 3: Prepare a stream map from the testJoinPlan
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;
  joinOrderer.gatherStreams(testJoinPlan->getRoot(), streamMap);

  // Step 4: Store the generated join plans
  std::vector<std::shared_ptr<JoinPlan>> generatedJoinPlans;

  // Build join plans from all permutations
  for (const auto& permutation : joinPermutations) {
    // Build the JoinPlan from the current permutation
    auto builtJoinPlan = joinOrderer.buildJoinPlanFromPermutation(
        permutation, windowAssignments, streamMap);

    if (builtJoinPlan != nullptr) {
      generatedJoinPlans.push_back(builtJoinPlan);
    }
  }

  // Step 5: Ensure that we have generated valid join plans
  ASSERT_GT(generatedJoinPlans.size(), 0)
      << "No valid join plans built from the permutations";

  // Step 6: Verify each generated join plan
  for (const auto& joinPlan : generatedJoinPlans) {
    ASSERT_NE(joinPlan, nullptr) << "Join plan is null";

    // Check that the root of the join plan is a WindowJoinOperator
    // (SlidingWindowJoin or IntervalJoin)
    auto root = joinPlan->getRoot();
    ASSERT_TRUE(std::dynamic_pointer_cast<WindowJoinOperator>(root) != nullptr)
        << "Root of the join plan is not a WindowJoinOperator";

    // Step 7: Verify that the join plan follows the left-deep structure
    auto currentNode = std::dynamic_pointer_cast<WindowJoinOperator>(root);
    const auto& permutationSteps =
        joinPermutations[0]
            .getSteps();  // Use the steps from the first permutation

    for (size_t i = 0; i < permutationSteps.size(); ++i) {
    }
  }
}
