#include "JoinOrderer.h"

#include <cxxabi.h>

#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
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

// Helper to split the join order name (e.g., "A_B_C" -> {"A", "B", "C"})
std::vector<std::string> splitJoinOrder(const std::string& joinOrder) {
  std::vector<std::string> streams;
  std::stringstream ss(joinOrder);
  std::string stream;
  while (std::getline(ss, stream, '_')) {
    streams.push_back(stream);
  }
  return streams;
}

std::string canonicalPair(const std::string& left, const std::string& right) {
  if (left < right) {
    return left + "_" + right;
  } else {
    return right + "_" + left;
  }
}

std::pair<std::vector<WindowSpecification>,
          std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                             std::vector<WindowSpecification>>>
JoinOrderer::getWindowSpecificationsAndAssignments(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  std::vector<WindowSpecification> windowSpecs;
  std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                     std::vector<WindowSpecification>>
      windowAssignments;

  auto currentNode = joinPlan->getRoot();

  // Traverse the join tree in a left-deep manner
  while (currentNode) {
    if (auto slidingJoin =
            std::dynamic_pointer_cast<SlidingWindowJoin>(currentNode)) {
      // Create a Sliding Window Specification
      WindowSpecification slidingSpec =
          WindowSpecification::createSlidingWindowSpecification(
              slidingJoin->getLength(), slidingJoin->getSlide());
      windowSpecs.emplace_back(slidingSpec);  // Add to the list of specs

      std::vector<WindowSpecification> slidingSpecs({slidingSpec});
      windowAssignments[slidingJoin] = slidingSpecs;  // Map to the operator

      currentNode = slidingJoin->getLeftChild();  // Move to the left child

    } else if (auto intervalJoin =
                   std::dynamic_pointer_cast<IntervalJoin>(currentNode)) {
      // Create an Interval Window Specification
      WindowSpecification intervalSpec =
          WindowSpecification::createIntervalWindowSpecification(
              intervalJoin->getLowerBound(), intervalJoin->getUpperBound());
      windowSpecs.emplace_back(intervalSpec);  // Add to the list of specs

      std::vector<WindowSpecification> intervalSpecs({intervalSpec});
      windowAssignments[intervalJoin] = intervalSpecs;  // Map to the operator

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

std::vector<std::shared_ptr<WindowJoinOperator>> JoinOrderer::decomposeJoinPair(
    const std::shared_ptr<WindowJoinOperator>& joinOperator) {
  std::vector<std::shared_ptr<WindowJoinOperator>> decomposedPairs;

  // TODO: Finish
}

void JoinOrderer::createCommutativePairs(
    std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                       std::vector<WindowSpecification>>& windowAssignments) {
  std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                     std::vector<WindowSpecification>>
      newAssignments;

  for (const auto& entry : windowAssignments) {
    const auto& joinOperator = entry.first;
    const auto& windowSpecs = entry.second;

    // Get the left and right children (streams) of the join operator
    auto leftChild = joinOperator->getLeftChild();
    auto rightChild = joinOperator->getRightChild();

    std::shared_ptr<WindowJoinOperator> commutativePair;

    // Check if it's a SlidingWindowJoin or an IntervalJoin
    if (auto slidingJoin =
            std::dynamic_pointer_cast<SlidingWindowJoin>(joinOperator)) {
      // Create commutative pair for SlidingWindowJoin
      commutativePair = std::make_shared<SlidingWindowJoin>(
          rightChild, leftChild, slidingJoin->getLength(),
          slidingJoin->getSlide(), slidingJoin->getTimeDomain(),
          slidingJoin->getTimestampPropagator());
    } else if (auto intervalJoin =
                   std::dynamic_pointer_cast<IntervalJoin>(joinOperator)) {
      // Create commutative pair for IntervalJoin
      commutativePair = std::make_shared<IntervalJoin>(
          rightChild, leftChild, intervalJoin->getLowerBound(),
          intervalJoin->getUpperBound(),
          intervalJoin->getTimestampPropagator());
    } else {
      // Unknown join type
      throw std::runtime_error(
          "Unknown join operator type when creating commutative pairs.");
    }

    // Add the commutative pair with the same window specs
    newAssignments[commutativePair] = windowSpecs;
  }

  // Merge the new commutative pairs into the original windowAssignments
  windowAssignments.insert(newAssignments.begin(), newAssignments.end());
}

void JoinOrderer::createUpdatedWindowAssignments(
    std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                       std::vector<WindowSpecification>>& windowAssignments,
    const std::unordered_map<WindowSpecification, std::string>&
        timePropagators) {
  // Iterate over the windowAssignments (join pairs and window specs)
  for (const auto& pair : windowAssignments) {
    auto joinOperator = pair.first;
    auto windowSpec = pair.second;

    bool leftChildIsAJoin = joinOperator->getLeftChild()
                                ->getOutputStream()
                                ->getBaseStreams()
                                .size() > 1;

    // If this doesn't work cast to WindowJoin. and check if that is a valid
    // ptr.

    if (leftChildIsAJoin) {
      // Operator has a joined pair as the left child.
      // Decompose the join pair, e.g., ABC -> AB, BC
      auto decomposedPairs =
          decomposeJoinPair(joinOperator);  // TODO: impl decompose.

      for (auto& decomposedPair : decomposedPairs) {
        // For each decomposedPair (joinPair) get the appropriate Window
        // Specification
        auto windowSpec = windowAssignments.at(decomposedPair);
        auto timePropagator = timePropagators.at(windowSpec.front());
        auto leftStreamName = decomposedPair->getLeftChild()->getName();

        assert(windowSpec.size() == 1);

        // CAUTION: This might be wrong
        // if left stream.size > 2
        if (leftStreamName == timePropagator) {
          windowAssignments[decomposedPair].push_back(windowSpec.front());
        } else {
          // Add other window specs as needed
          for (const auto& [joinOp, ws] : windowAssignments) {
            // Ensure you're pushing individual WindowSpecification elements
            for (const auto& singleSpec : ws) {
              windowAssignments[decomposedPair].push_back(
                  singleSpec);  // TODO: Discuss with Ariane.
            }
          }
        }
      }
    }
  }
  createCommutativePairs(windowAssignments);
}

// Clean solution for reordering.
// 1. Get window specifications [x]
// 2. Get window assignments [x]
// 3. Get Propagators [x]
// 4. Check TimeDomain [x]
// 5.1 Call PT Orderer [x] (misses LWO...)
// 5.2 Call ET Orderer [x]
// 6. Generate Permutations for Joins. []
// 7. Iterate over Permutations []
// 7.1 Try to assign Window Operator []
// 7.1.1 fail => return []
// 7.1.2 Continue if there are more joins, else return that as a proper []
// reordering.
std::vector<std::shared_ptr<JoinPlan>> JoinOrderer::reorder(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  // Setup: Get WindowSpecs, Assignments, Propagators and TimeDomain
  auto [windowSpecs, windowAssignments] =
      getWindowSpecificationsAndAssignments(joinPlan);

  auto timeDomain = joinPlan->getTimeDomain();

  auto timestampPropagators = getTimestampPropagators(joinPlan, windowSpecs);

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
    createUpdatedWindowAssignments(windowAssignments, timestampPropagators);
  }

  // TODO: Proceed with updatedWindowAssignments.

  return std::vector<std::shared_ptr<JoinPlan>>();  // TODO: Change
}

// Function to reorder the joins and return new JoinPlans
// std::vector<std::shared_ptr<JoinPlan>> JoinOrderer::reorder(
//     const std::shared_ptr<JoinPlan>& joinPlan) {
//   const auto timeDomain = joinPlan->getTimeDomain();
//   std::vector<std::shared_ptr<JoinPlan>> reorderedPlans;
//   std::set<std::string> seenPairs;
//   std::shared_ptr<Node> root = joinPlan->getRoot();
//   std::unordered_map<std::string, std::shared_ptr<Stream>> streamMap;
//   std::vector<std::string> streamNames;
//   std::vector<std::vector<std::string>> permutations;
//   std::string timestampPropagator;

//   // Gather all streams involved in the join
//   gatherStreams(root, streamMap);

//   // Extract the names of the streams for permutation generation
//   for (const auto& entry : streamMap) {
//     streamNames.push_back(entry.first);
//   }

//   // Generate all possible permutations of the current streams
//   generatePermutations(streamNames, permutations);

//   // Window properties
//   long length = 0, slide = 0;
//   long lowerBound = 0, upperBound = 0;
//   bool isSlidingWindow = false;

//   // Check if the root node is a SlidingWindowJoin or IntervalJoin
//   if (auto slidingJoin =
//   std::dynamic_pointer_cast<SlidingWindowJoin>(root))
//   {
//     length = slidingJoin->getLength();
//     slide = slidingJoin->getSlide();
//     isSlidingWindow = true;
//   } else if (auto intervalJoin =
//                  std::dynamic_pointer_cast<IntervalJoin>(root)) {
//     lowerBound = intervalJoin->getLowerBound();
//     upperBound = intervalJoin->getUpperBound();
//     isSlidingWindow = false;
//   }

//   // Iterate over permutations to create new join plans
//   for (const auto& perm : permutations) {
// #if DEBUG_MODE
//     for (const auto& streamName : perm) {
//       std::cout << streamName << ", ";
//     }
//     std::cout << '\n';
// #endif

//     std::shared_ptr<WindowJoinOperator> join;

//     // Canonical pair check for commutative join avoidance (only for
//     // SlidingWindowJoin)
//     std::string firstPair = canonicalPair(perm[0], perm[1]);

//     // Use the helper function to prune invalid plans
//     if (isPruneablePlan(firstPair, root, perm, seenPairs, isSlidingWindow))
//     {
//       continue;  // Skip this permutation since it's pruneable
//     }

//     // Get left and right streams from the stream map
//     std::shared_ptr<Stream> leftStream = streamMap.at(perm[0]);
//     std::shared_ptr<Stream> rightStream = streamMap.at(perm[1]);

//     if (auto slidingJoin =
//     std::dynamic_pointer_cast<SlidingWindowJoin>(root)) {
//       timestampPropagator = slidingJoin->getTimestampPropagator();
//     } else if (auto intervalJoin =
//                    std::dynamic_pointer_cast<IntervalJoin>(root)) {
//       timestampPropagator = intervalJoin->getTimestampPropagator();
//     } else {
//       throw std::runtime_error("Unsupported join type in reordering.");
//     }

//     // Start with the appropriate join operator (SlidingWindowJoin or
//     // IntervalJoin)
//     if (isSlidingWindow) {
//       join = std::make_shared<SlidingWindowJoin>(
//           leftStream, rightStream, length, slide, timeDomain,
//           timeDomain == TimeDomain::EVENT_TIME ? timestampPropagator :
//           "NONE");
//     } else {
//       // For IntervalJoin, if commutative pair detected, swap bounds
//       long newLowerBound = lowerBound;
//       long newUpperBound = upperBound;

//       if (lowerBound != upperBound && perm[0] != timestampPropagator) {
//         // Swap bounds if order is reversed for IntervalJoin
//         std::swap(newLowerBound, newUpperBound);
//       }

//       join =
//           std::make_shared<IntervalJoin>(leftStream, rightStream,
//           newLowerBound,
//                                          newUpperBound,
//                                          timestampPropagator);
//     }

//     // Continue joining with remaining streams (left-deep join)
//     for (size_t i = 2; i < perm.size(); ++i) {
//       std::shared_ptr<Stream> nextStream = streamMap.at(perm[i]);

//       if (isSlidingWindow) {
//         join = std::make_shared<SlidingWindowJoin>(
//             join, nextStream, length, slide, timeDomain,
//             timeDomain == TimeDomain::EVENT_TIME ? timestampPropagator
//                                                  : "NONE");
//       } else {
//         join = std::make_shared<IntervalJoin>(join, nextStream, lowerBound,
//                                               upperBound,
//                                               timestampPropagator);
//       }
//     }

//     // Create a new JoinPlan for the reordered join
//     auto newJoinPlan = std::make_shared<JoinPlan>(join);

//     // Add the plan to the reorderedPlans
//     reorderedPlans.push_back(newJoinPlan);
//   }

//   return reorderedPlans;
// }

// bool JoinOrderer::isPruneablePlan(const std::string& firstPair,
//                                   const std::shared_ptr<Node>& root,
//                                   const std::vector<std::string>& perm,
//                                   std::set<std::string>& seenPairs,
//                                   bool isSlidingWindow) {
//   // Apply commutative pair pruning only for SlidingWindowJoin
//   if (isSlidingWindow) {
//     if (seenPairs.find(firstPair) != seenPairs.end()) {
//       return true;  // Skip redundant permutations
//     }
//     seenPairs.insert(firstPair);
//   }

//   // Cast root to WindowJoinOperator to get the timestamp propagator
//   auto rootJoin = std::dynamic_pointer_cast<WindowJoinOperator>(root);
//   if (!rootJoin) {
//     throw std::runtime_error("Root node is not a WindowJoinOperator");
//   }

//   const auto& timestampPropagator = rootJoin->getTimestampPropagator();

//   // Pruning logic: Ensure the permutation has a valid timestamp propagator
//   if (perm[0] != timestampPropagator && perm[1] != timestampPropagator) {
//     if (rootJoin->getTimeDomain() == TimeDomain::EVENT_TIME) {
//       return true;  // Skip this permutation since neither matches the
//       timestamp
//                     // propagator, and its not processing time
//     }
//   }

//   return false;  // This plan is not pruneable
// }
