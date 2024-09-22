#include "JoinOrderer.h"

#include <cxxabi.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

#include "IntervalJoin.h"
#include "SlidingWindowJoin.h"
#include "Stream.h"
#include "TimeDomain.h"

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
                             WindowSpecification>>
JoinOrderer::getWindowSpecificationsAndAssignments(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  std::vector<WindowSpecification> windowSpecs;
  std::unordered_map<std::shared_ptr<WindowJoinOperator>, WindowSpecification>
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

      windowSpecs.emplace_back(slidingSpec);         // Add to the list of specs
      windowAssignments[slidingJoin] = slidingSpec;  // Map to the operator

      currentNode = slidingJoin->getLeftChild();  // Move to the left child

    } else if (auto intervalJoin =
                   std::dynamic_pointer_cast<IntervalJoin>(currentNode)) {
      // Create an Interval Window Specification
      WindowSpecification intervalSpec =
          WindowSpecification::createIntervalWindowSpecification(
              intervalJoin->getLowerBound(), intervalJoin->getUpperBound());

      windowSpecs.emplace_back(intervalSpec);  // Add to the list of specs
      windowAssignments[intervalJoin] = intervalSpec;  // Map to the operator

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

std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                   std::vector<WindowSpecification>>
JoinOrderer::createUpdatedWindowAssignments(
    const std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                             WindowSpecification>& windowAssignments,
    const std::unordered_map<WindowSpecification, std::string>&
        timePropagators) {
  std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                     std::vector<WindowSpecification>>
      updatedWindowAssignments;

  // Iterate over the windowAssignments (join pairs and window specs)
  for (const auto& pair : windowAssignments) {
    auto joinOperator = pair.first;
    auto windowSpec = pair.second;

    if (joinOperator->getLeftChild()
            ->getOutputStream()
            ->getBaseStreams()
            .size() > 1) {
      // Decompose the join pair, e.g., ABC -> AB, BC
      auto decomposedPairs = decomposeJoinPair(
          joinOperator);  // TODO: Find a way to decompose here.

      // TODO: Continue with decomposed Pair
    }
  }

  // Get Commutations ? but do we always get them? YES!!

  return updatedWindowAssignments;
}

// Clean solution for reordering.
// 1. Get window specifications [x]
// 2. Get window assignments [x]
// 3. Get Propagators [x]
// 4. Check TimeDomain [x]
// 5.1 Call PT Orderer [x] (misses LWO...)
// 5.2 Call ET Orderer []
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

  // Declare, fill later with other possible window assignments for a join.
  std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                     std::vector<WindowSpecification>>
      updatedWindowAssignments;

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
    updatedWindowAssignments =
        createUpdatedWindowAssignments(windowAssignments, timestampPropagators);
  }

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
