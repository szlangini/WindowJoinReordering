#include "JoinOrderer.h"

#include <cxxabi.h>

#include <iostream>
#include <memory>
#include <sstream>
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

// Function to reorder the joins and return new JoinPlans
std::vector<std::shared_ptr<JoinPlan>> JoinOrderer::reorder(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  const auto timeDomain = joinPlan->getTimeDomain();
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans;
  std::set<std::string> seenPairs;
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

  // Window properties
  long length = 0, slide = 0;
  long lowerBound = 0, upperBound = 0;
  bool isSlidingWindow = false;

  // Check if the root node is a SlidingWindowJoin or IntervalJoin
  if (auto slidingJoin = std::dynamic_pointer_cast<SlidingWindowJoin>(root)) {
    length = slidingJoin->getLength();
    slide = slidingJoin->getSlide();
    isSlidingWindow = true;
  } else if (auto intervalJoin =
                 std::dynamic_pointer_cast<IntervalJoin>(root)) {
    lowerBound = intervalJoin->getLowerBound();
    upperBound = intervalJoin->getUpperBound();
    isSlidingWindow = false;
  }

  // Iterate over permutations to create new join plans
  for (const auto& perm : permutations) {
#if DEBUG_MODE
    for (const auto& streamName : perm) {
      std::cout << streamName << ", ";
    }
    std::cout << '\n';
#endif

    std::shared_ptr<WindowJoinOperator> join;

    // Canonical pair check for commutative join avoidance (only for
    // SlidingWindowJoin)
    std::string firstPair = canonicalPair(perm[0], perm[1]);

    // Use the helper function to prune invalid plans
    if (isPruneablePlan(firstPair, root, perm, seenPairs, isSlidingWindow)) {
      continue;  // Skip this permutation since it's pruneable
    }

    // Get left and right streams from the stream map
    std::shared_ptr<Stream> leftStream = streamMap.at(perm[0]);
    std::shared_ptr<Stream> rightStream = streamMap.at(perm[1]);

    // Start with the appropriate join operator (SlidingWindowJoin or
    // IntervalJoin)
    if (isSlidingWindow) {
      join = std::make_shared<SlidingWindowJoin>(
          leftStream, rightStream, length, slide, timeDomain,
          timeDomain == TimeDomain::EVENT_TIME ? perm[0] : "NONE");
    } else {
      // For IntervalJoin, if commutative pair detected, swap bounds if
      // necessary
      long newLowerBound = lowerBound;
      long newUpperBound = upperBound;

      if (lowerBound != upperBound &&
          perm[0] != root->getTimestampPropagator()) {
        // Swap bounds if order is reversed for IntervalJoin
        std::swap(newLowerBound, newUpperBound);
      }

      join = std::make_shared<IntervalJoin>(
          leftStream, rightStream, newLowerBound, newUpperBound, perm[0]);
    }

    // Continue joining with remaining streams (left-deep join)
    for (size_t i = 2; i < perm.size(); ++i) {
      std::shared_ptr<Stream> nextStream = streamMap.at(perm[i]);

      if (isSlidingWindow) {
        join = std::make_shared<SlidingWindowJoin>(
            join, nextStream, length, slide, timeDomain,
            timeDomain == TimeDomain::EVENT_TIME ? perm[0] : "NONE");
      } else {
        join = std::make_shared<IntervalJoin>(join, nextStream, lowerBound,
                                              upperBound, perm[0]);
      }
    }

    // Create a new JoinPlan for the reordered join
    auto newJoinPlan = std::make_shared<JoinPlan>(join);

    // Add the plan to the reorderedPlans
    reorderedPlans.push_back(newJoinPlan);
  }

  return reorderedPlans;
}

bool JoinOrderer::isPruneablePlan(const std::string& firstPair,
                                  const std::shared_ptr<Node>& root,
                                  const std::vector<std::string>& perm,
                                  std::set<std::string>& seenPairs,
                                  bool isSlidingWindow) {
  // Apply commutative pair pruning only for SlidingWindowJoin
  if (isSlidingWindow) {
    if (seenPairs.find(firstPair) != seenPairs.end()) {
      return true;  // Skip redundant permutations
    }
    seenPairs.insert(firstPair);
  }

  // Cast root to WindowJoinOperator to get the timestamp propagator
  auto rootJoin = std::dynamic_pointer_cast<WindowJoinOperator>(root);
  if (!rootJoin) {
    throw std::runtime_error("Root node is not a WindowJoinOperator");
  }

  const auto& timestampPropagator = rootJoin->getTimestampPropagator();

  // Pruning logic: Ensure the permutation has a valid timestamp propagator
  if (perm[0] != timestampPropagator && perm[1] != timestampPropagator) {
    return true;  // Skip this permutation since neither matches the timestamp
                  // propagator
  }

  return false;  // This plan is not pruneable
}
