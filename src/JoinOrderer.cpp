#include "JoinOrderer.h"

#include <cxxabi.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "IntervalJoin.h"
#include "SlidingWindowJoin.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "WindowJoinOperator.h"
#include "WindowSpecification.h"

#define DEBUG_MODE 0

std::string demangle(const char* mangledName) {
  int status = -1;
  // Call abi::__cxa_demangle to demangle the type name
  std::unique_ptr<char, void (*)(void*)> result(
      abi::__cxa_demangle(mangledName, nullptr, nullptr, &status), std::free);
  return (status == 0) ? result.get()
                       : mangledName;  // Return demangled name if successful
}

void JoinOrderer::gatherStreams(
    const std::shared_ptr<Node>& node,
    std::unordered_map<std::string, std::shared_ptr<Stream>>& streamMap) {
  if (auto stream = std::dynamic_pointer_cast<Stream>(node)) {
    // Store the stream by its name
    streamMap[stream->getName()] = stream;
  } else if (auto slidingJoin =
                 std::dynamic_pointer_cast<SlidingWindowJoin>(node)) {
    // Recursively gather streams from the left and right children of the join
    gatherStreams(slidingJoin->getLeftChild(), streamMap);
    gatherStreams(slidingJoin->getRightChild(), streamMap);
  } else if (auto intervalJoin =
                 std::dynamic_pointer_cast<IntervalJoin>(node)) {
    // Handle IntervalJoin: Recursively gather streams from left and right
    // children
    gatherStreams(intervalJoin->getLeftChild(), streamMap);
    gatherStreams(intervalJoin->getRightChild(), streamMap);
  } else {
    // Demangle the class name for a more readable error message
    std::string typeName = demangle(typeid(*node).name());
    throw std::runtime_error(
        "Unknown node type encountered in join tree: " + node->getName() +
        ". Actual type: " + typeName +
        ". Expected either a Stream, SlidingWindowJoin, or IntervalJoin.");
  }
}

// Helper to generate all permutations of streams
void JoinOrderer::generatePermutations(
    const std::vector<std::string>& streams,
    std::vector<std::vector<std::string>>& permutations) {
  std::vector<std::string> perm = streams;
  // Sort the streams to ensure the permutations are generated correctly
  std::sort(perm.begin(), perm.end());
  do {
    permutations.push_back(perm);
  } while (std::next_permutation(perm.begin(), perm.end()));
}

std::pair<std::vector<WindowSpecification>,
          std::unordered_map<JoinKey, std::vector<WindowSpecification>>>
JoinOrderer::getWindowSpecificationsAndAssignments(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  std::vector<WindowSpecification> windowSpecs;
  std::unordered_map<JoinKey, std::vector<WindowSpecification>>
      windowAssignments;

  auto currentNode = joinPlan->getRoot();

  JoinKey joinKey;  // AB:C_W2 & A:B_W1 will be emitted from ABC

  // Traverse the join tree in a left-deep manner
  while (currentNode) {
    if (auto slidingJoin =
            std::dynamic_pointer_cast<SlidingWindowJoin>(currentNode)) {
      // Create a Sliding Window Specification
      WindowSpecification slidingSpec =
          WindowSpecification::createSlidingWindowSpecification(
              slidingJoin->getLength(), slidingJoin->getSlide());
      windowSpecs.emplace_back(slidingSpec);  // Add to the list of specs

      // Fill Join Key
      joinKey.joinType = JoinType::SlidingWindowJoin;
      joinKey.leftStreams =
          slidingJoin->getLeftChild()->getOutputStream()->getBaseStreams();
      joinKey.rightStreams =
          slidingJoin->getRightChild()->getOutputStream()->getBaseStreams();

      std::vector<WindowSpecification> slidingSpecs({slidingSpec});
      windowAssignments[joinKey] = slidingSpecs;  // Map to the operator

      currentNode = slidingJoin->getLeftChild();  // Move to the left child

    } else if (auto intervalJoin =
                   std::dynamic_pointer_cast<IntervalJoin>(currentNode)) {
      // Create an Interval Window Specification
      WindowSpecification intervalSpec =
          WindowSpecification::createIntervalWindowSpecification(
              intervalJoin->getLowerBound(), intervalJoin->getUpperBound());
      windowSpecs.emplace_back(intervalSpec);  // Add to the list of specs

      // Fill Join Key
      joinKey.joinType = JoinType::IntervalJoin;
      joinKey.leftStreams =
          intervalJoin->getLeftChild()->getOutputStream()->getBaseStreams();
      joinKey.rightStreams = intervalJoin->getOutputStream()->getBaseStreams();

      std::vector<WindowSpecification> intervalSpecs({intervalSpec});
      windowAssignments[joinKey] = intervalSpecs;  // Map to the operator

      currentNode = intervalJoin->getLeftChild();  // Move to the left child

    } else {
      throw std::runtime_error(
          "Current node is not a WindowJoinOperator, cannot retrieve Window "
          "Specifications");
    }
  }

  // Reverse the window specs to ensure W1 corresponds to the first join
  std::reverse(windowSpecs.begin(), windowSpecs.end());

  return {windowSpecs, windowAssignments};
}

// Function to return a map from WindowSpecification to time propagator
std::unordered_map<WindowSpecification, std::string>
JoinOrderer::getTimestampPropagators(
    const std::shared_ptr<JoinPlan>& joinPlan,
    const std::vector<WindowSpecification>& windowSpecs) {
  std::vector<std::string> timestampPropagators;
  std::unordered_map<WindowSpecification, std::string> timePropagatorMap;
  auto currentNode = joinPlan->getRoot();

  // Step 1: Traverse the join tree in a left-deep manner and collect timestamp
  // propagators
  while (currentNode) {
    if (auto slidingJoin =
            std::dynamic_pointer_cast<SlidingWindowJoin>(currentNode)) {
      timestampPropagators.push_back(slidingJoin->getTimestampPropagator());
      currentNode = slidingJoin->getLeftChild();  // Move to the left child
    } else if (auto intervalJoin =
                   std::dynamic_pointer_cast<IntervalJoin>(currentNode)) {
      timestampPropagators.push_back(intervalJoin->getTimestampPropagator());
      currentNode = intervalJoin->getLeftChild();  // Move to the left child
    } else {
      throw std::runtime_error("Unknown node type encountered in join tree");
    }
  }

  // Step 2: Reverse the timestampPropagators vector to align with the
  // windowSpecs order
  std::reverse(timestampPropagators.begin(), timestampPropagators.end());

  // Step 3: Create a map of WindowSpecification to timestamp propagator
  if (timestampPropagators.size() != windowSpecs.size()) {
    throw std::runtime_error(
        "Mismatch between window specifications and timestamp propagators.");
  }

  for (size_t i = 0; i < windowSpecs.size(); ++i) {
    timePropagatorMap[windowSpecs[i]] = timestampPropagators[i];
  }

  return timePropagatorMap;
}

std::vector<std::shared_ptr<JoinPlan>>
JoinOrderer::getAllSlidingWindowJoinPermutations(
    const std::shared_ptr<JoinPlan>& joinPlan,
    const WindowSpecification generalWindowSpec) {
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans;
  std::shared_ptr<Node> root = joinPlan->getRoot();
  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;
  std::vector<std::string> streamNames;
  std::vector<std::vector<std::string>> permutations;

  // Gather all streams involved in the join
  gatherStreams(root, streamMap);

  // Extract the names of the streams for permutation generation
  for (const auto& entry : streamMap) {
    streamNames.push_back(entry.first);
  }

  // Generate all possible permutations of the current streams
  generatePermutations(streamNames, permutations);

  // Window properties from the general window specification
  long length = generalWindowSpec.length;
  long slide = generalWindowSpec.slide;
  auto timeDomain = TimeDomain::PROCESSING_TIME;  // A2 case is Processing Time

  // Iterate over permutations to create new join plans
  for (const auto& perm : permutations) {
    std::shared_ptr<SlidingWindowJoin> join;

    // Get left and right streams from the stream map
    std::shared_ptr<Stream> leftStream = streamMap.at(perm[0]);
    std::shared_ptr<Stream> rightStream = streamMap.at(perm[1]);

    // Start with the SlidingWindowJoin for the first two streams
    join = std::make_shared<SlidingWindowJoin>(leftStream, rightStream, length,
                                               slide, timeDomain, "NONE");

    // Continue joining with remaining streams (left-deep join)
    for (size_t i = 2; i < perm.size(); ++i) {
      std::shared_ptr<Stream> nextStream = streamMap.at(perm[i]);
      join = std::make_shared<SlidingWindowJoin>(join, nextStream, length,
                                                 slide, timeDomain, "NONE");
    }

    // Create a new JoinPlan for the reordered join
    auto newJoinPlan = std::make_shared<JoinPlan>(join);

    // Add the plan to the reorderedPlans
    reorderedPlans.push_back(newJoinPlan);
  }

  return reorderedPlans;
}

std::vector<JoinKey> JoinOrderer::decomposeJoinPair(const JoinKey& joinKey) {
  std::vector<JoinKey> decomposedPairs;

  // Extract base streams from the left child
  const auto& leftStreams = joinKey.leftStreams;
  const auto& rightStream = joinKey.rightStreams;

  // Ensure the left child is a join of at least two base streams
  assert(joinKey.leftStreams.size() > 1);

  // Ensure the right child is a stream (not a join operator)
  assert(joinKey.rightStreams.size() == 1);

  // Loop through the base streams in the left child
  for (const auto& stream : leftStreams) {
    // Create a new join between each stream and the right child
    std::unordered_set<std::string> newLeftStreams = {stream};
    std::unordered_set<std::string> newRightStreams = rightStream;

    // Create a new JoinKey for the decomposed join pair
    JoinKey newJoinKey;
    newJoinKey.leftStreams = newLeftStreams;
    newJoinKey.rightStreams = newRightStreams;
    newJoinKey.joinType = joinKey.joinType;  // Retain the original join type

    // Add the new decomposed JoinKey to the list of decomposed pairs
    decomposedPairs.push_back(newJoinKey);
  }

  return decomposedPairs;
}

void JoinOrderer::createCommutativePairs(
    std::unordered_map<JoinKey, std::vector<WindowSpecification>>&
        windowAssignments) {
  // Create a new container for commutative pairs
  std::unordered_map<JoinKey, std::vector<WindowSpecification>> newAssignments;

  for (const auto& entry : windowAssignments) {
    const auto& joinKey = entry.first;  // Original join pair
    const auto& windowSpecs =
        entry.second;  // Corresponding window specifications

    // Create a commutative pair by swapping the left and right streams
    JoinKey commutativePair;
    commutativePair.leftStreams = joinKey.rightStreams;  // Swap left and right
    commutativePair.rightStreams = joinKey.leftStreams;  // Swap right and left
    commutativePair.joinType = joinKey.joinType;  // Keep the original join type

    // Add the commutative pair with the same window specifications
    newAssignments[commutativePair] = windowSpecs;
  }

  // Merge the new commutative pairs into the original windowAssignments
  windowAssignments.insert(newAssignments.begin(), newAssignments.end());
}

// Input are AB:C with Window 2 and A:B with Window 1
void JoinOrderer::createUpdatedWindowAssignments(
    std::unordered_map<JoinKey, std::vector<WindowSpecification>>&
        windowAssignments,
    const std::unordered_map<WindowSpecification, std::string>&
        timePropagators) {
  // Iterate over the windowAssignments (join pairs and window specs)
  for (const auto& pair : windowAssignments) {  // AB:C_W2 und A:B_W1
    auto joinKey = pair.first;
    auto windowSpec = pair.second;

    auto leftChildIsAJoin = joinKey.leftStreams.size() > 1;

    if (leftChildIsAJoin) {  // Only AB:C
      // Operator has a joined pair as the left child.
      // Decompose the join pair, e.g., ABC -> AC, BC
      auto decomposedPairs = decomposeJoinPair(joinKey);  // AC, BC

      for (auto& decomposedPair : decomposedPairs) {
        // For each decomposedPair (joinPair) get the appropriate Window
        // Specification
        auto windowSpec = windowAssignments.at(
            joinKey);  // Assign for the decomposed pairs the same window as for
                       // the two-way join AB:C_W2 => AC_w2, BC_w2

        assert(windowSpec.size() > 0);  // Make sure we have windowSpecs

        auto timePropagator = timePropagators.at(windowSpec.front());

        // Check the left stream's name in the decomposed pair
        const auto& leftStreamNames = decomposedPair.leftStreams;

        if (leftStreamNames.find(timePropagator) != leftStreamNames.end()) {
          windowAssignments[decomposedPair].push_back(windowSpec.front());
        } else {
          // Add other window specs as needed
          // TODO: Discuss with Ariane why we do this, couldn't we just leave
          // that empty?
          for (const auto& [key, ws] : windowAssignments) {
            for (const auto& spec : ws) {
              windowAssignments[decomposedPair].push_back(spec);
            }
          }
        }
      }
    }
  }
  createCommutativePairs(windowAssignments);
}

std::vector<JoinPermutation> JoinOrderer::generateAllJoinPermutations(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  // Get joinType
  auto joinType = joinPlan->getJoinType();

  // Get the base streams from the join plan root
  std::unordered_set<std::string> baseStreams =
      joinPlan->getRoot()->getOutputStream()->getBaseStreams();

  std::vector<std::string> streamNames(baseStreams.begin(), baseStreams.end());

  // Generate all permutations of the streams
  std::vector<std::vector<std::string>> permutations;
  generatePermutations(streamNames, permutations);

  // Hold all the JoinPermutations we will generate
  std::vector<JoinPermutation> allPermutations;

  // Iterate over each permutation to create JoinPermutations
  for (const auto& perm : permutations) {
    JoinPermutation permutation;

    // Iterate over the pairs of streams in the permutation and build the join
    for (size_t i = 0; i < perm.size() - 1; ++i) {
      std::string left = perm[i];
      std::string right = perm[i + 1];

      // Create a new JoinKey for this pair
      JoinKey joinKey(joinType, {left},
                      {right});  // ABC will become: AB and BC keys. BAC will
                                 // become: BA and AC...

      // Add this step to the permutation
      permutation.addJoinStep(joinKey);
    }

    // Add the generated permutation to the list of all permutations
    allPermutations.push_back(permutation);
  }

  return allPermutations;
}

std::shared_ptr<JoinPlan> JoinOrderer::buildJoinPlanFromPermutation(
    const JoinPermutation& permutation,
    const std::unordered_map<JoinKey, std::vector<WindowSpecification>>&
        windowAssignments,
    const std::unordered_map<std::string, std::shared_ptr<Stream>>& streamMap,
    const std::unordered_map<WindowSpecification, std::string>&
        timestampPropagators) {
  std::shared_ptr<WindowJoinOperator> currentJoin = nullptr;

  // Iterate over the steps in the permutation
  for (size_t i = 0; i < permutation.getSteps().size(); ++i) {
    const JoinKey& joinKey = permutation.getSteps()[i];

    // Retrieve window specifications for the current join
    const auto& windowSpecs = windowAssignments.at(joinKey);
    if (windowSpecs.size() != 1) {
      return nullptr;  // Invalid case, we should always expect one spec per
                       // join step
    }
    const auto& windowSpec = windowSpecs.front();
    auto timestampPropagator = timestampPropagators.at(windowSpec);

    // Ensure the left and right streams in the join key are present in the
    // stream map
    assert(joinKey.leftStreams.size() == 1);
    assert(joinKey.rightStreams.size() == 1);

    std::shared_ptr<Node> leftChild;
    std::shared_ptr<Node> rightChild =
        streamMap.at(*joinKey.rightStreams.begin());

    // First join: Use streams from the streamMap
    if (i == 0) {
      leftChild = streamMap.at(*joinKey.leftStreams.begin());
    }
    // Subsequent joins: Use the current join as the left child
    else {
      leftChild = currentJoin;  // Use the accumulated join
    }

    // Use the join type to create the appropriate join operator
    if (joinKey.joinType == JoinType::SlidingWindowJoin) {
      currentJoin = std::make_shared<SlidingWindowJoin>(
          leftChild,   // Left child (either stream or previous join result)
          rightChild,  // Right child (stream)
          windowSpec.length,       // Sliding Window Length
          windowSpec.slide,        // Sliding Window Slide
          TimeDomain::EVENT_TIME,  // Assuming Event Time, change as needed
          timestampPropagator      // Timestamp propagator
      );
    } else if (joinKey.joinType == JoinType::IntervalJoin) {
      currentJoin = std::make_shared<IntervalJoin>(
          leftChild,   // Left child (either stream or previous join result)
          rightChild,  // Right child (stream)
          windowSpec.lowerBound,  // Interval lower bound
          windowSpec.upperBound,  // Interval upper bound
          timestampPropagator     // Timestamp propagator
      );
    } else {
      throw std::runtime_error("Unknown JoinType");
    }
  }

  // Return the final JoinPlan built from the permutation
  return std::make_shared<JoinPlan>(currentJoin);
}
std::vector<std::shared_ptr<JoinPlan>> JoinOrderer::reorder(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  // Setup: Get WindowSpecs, Assignments, Propagators and TimeDomain
  auto [windowSpecs, windowAssignments] =
      getWindowSpecificationsAndAssignments(joinPlan);

  auto timeDomain = joinPlan->getTimeDomain();

  auto timestampPropagators = getTimestampPropagators(joinPlan, windowSpecs);

  std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;
  gatherStreams(joinPlan->getRoot(), streamMap);

  // Early return if there is a problem retrieving the values.
  if (windowSpecs.empty() || windowAssignments.empty() ||
      timestampPropagators.empty()) {
    throw std::runtime_error(
        "Error: One or more required fields are not properly filled.");
  }

  if (timeDomain == TimeDomain::PROCESSING_TIME) {
    auto isCaseA4 = false;  // LWO needs sync with Ariane :)
    if (isCaseA4) {
    } else {
      return getAllSlidingWindowJoinPermutations(
          joinPlan,
          windowSpecs[0]);  // WindowSpecs are all equal -> We can do
                            // what we want. All orders are allowed in
                            // A2 PT. We want to return them all!
    }
  } else {
    // TimeDomain::EVENT_TIME

    // ABC_w2 -> AC_w2, BC_w2 -- Timestamp filter => AC_W2. Finally we have
    // AB_w1 AC_w2 with their commutations. That we can reorder to:
    // (ABC, BAC, CAB, ACB, [but not BCA or CBA])
    createUpdatedWindowAssignments(windowAssignments, timestampPropagators);
  }

  // Generate all possible join combinations
  std::vector<std::shared_ptr<JoinPlan>> validJoinPlans;
  auto joinPermutations = generateAllJoinPermutations(
      joinPlan);  // Permutation looks like AB, BC or BA, AC

  for (const auto& perm : joinPermutations) {
    std::shared_ptr<JoinPlan> newPlan = buildJoinPlanFromPermutation(
        perm, windowAssignments, streamMap, timestampPropagators);
    if (newPlan) {  // might be nullptr!
      validJoinPlans.push_back(newPlan);
    }
  }

  return validJoinPlans;
}
