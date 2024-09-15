#include "JoinOrderer.h"

#include <cxxabi.h>

#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "SlidingWindowJoin.h"
#include "Stream.h"

#define DEBUG_MODE 0

std::string demangle(const char* mangledName) {
  int status = -1;
  // Call abi::__cxa_demangle to demangle the type name
  std::unique_ptr<char, void (*)(void*)> result(
      abi::__cxa_demangle(mangledName, nullptr, nullptr, &status), std::free);
  return (status == 0) ? result.get()
                       : mangledName;  // Return demangled name if successful
}

void JoinOrderer::gatherStreams(const std::shared_ptr<Node>& node,
                                std::vector<std::shared_ptr<Stream>>& streams) {
  if (auto stream = std::dynamic_pointer_cast<Stream>(node)) {
    streams.push_back(stream);
  } else if (auto slidingJoin =
                 std::dynamic_pointer_cast<SlidingWindowJoin>(node)) {
    gatherStreams(slidingJoin->getLeftChild(), streams);
    gatherStreams(slidingJoin->getRightChild(), streams);
  } else {
    // Demangle the class name for a more readable error message
    std::string typeName = demangle(typeid(*node).name());
    throw std::runtime_error(
        "Unknown node type encountered in join tree: " + node->getName() +
        ". Actual type: " + typeName +
        ". Expected either a Stream or SlidingWindowJoin.");
  }
}

// Helper to generate all permutations of streams
void JoinOrderer::generatePermutations(
    const std::vector<std::string>& streams,
    std::vector<std::vector<std::string>>& permutations) {
  std::vector<std::string> perm = streams;
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

bool JoinOrderer::prunePermutations(const std::shared_ptr<JoinPlan>& joinPlan) {
  return true;  // TODO: Change
}

// Function to reorder the joins and return new JoinPlans
std::vector<std::shared_ptr<JoinPlan>> JoinOrderer::reorder(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans;

  // Get the current root node of the join plan and gather all streams
  std::shared_ptr<Node> root = joinPlan->getRoot();
  std::vector<std::shared_ptr<Stream>> streams;
  gatherStreams(root, streams);

  // Extract the names of the streams for permutation generation
  std::vector<std::string> streamNames;
  for (const auto& stream : streams) {
    streamNames.push_back(stream->getName());
  }

  // Generate all possible permutations of the current streams
  std::vector<std::vector<std::string>> permutations;
  generatePermutations(streamNames, permutations);

  // Get window properties from the root node if it's a SlidingWindowJoin
  long length;
  long slide;

  if (auto slidingJoin = std::dynamic_pointer_cast<SlidingWindowJoin>(root)) {
    length = slidingJoin->getLength();  // Dynamically get the length
    slide = slidingJoin->getSlide();    // Dynamically get the slide
  }

  // Create new join plans for each permutation, pruning invalid ones
  for (const auto& perm : permutations) {
#if DEBUG_MODE
    for (const auto& streamName : perm) {
      std::cout << streamName << ", ";
    }
    std::cout << '\n';

#endif
    std::shared_ptr<SlidingWindowJoin> join;

    // Rebuild the join tree based on the current permutation of streams
    // TODO: Find bug that prevents reordered JoinPlan from having tuples
    std::shared_ptr<Stream> leftStream = std::make_shared<Stream>(perm[0]);
    std::shared_ptr<Stream> rightStream = std::make_shared<Stream>(perm[1]);

    // Start with a binary join for the first two streams using dynamic window
    // properties
    join = std::make_shared<SlidingWindowJoin>(leftStream, rightStream, length,
                                               slide, perm[0]);

    // Continue joining with remaining streams in the permutation
    for (size_t i = 2; i < perm.size(); ++i) {
      std::shared_ptr<Stream> nextStream = std::make_shared<Stream>(perm[i]);
      join = std::make_shared<SlidingWindowJoin>(join, nextStream, length,
                                                 slide, perm[0]);
    }

    // Create a new JoinPlan for the reordered join
    auto newJoinPlan = std::make_shared<JoinPlan>(join);

    // Prune invalid join plans
    if (prunePermutations(newJoinPlan)) {
      reorderedPlans.push_back(newJoinPlan);
    }
  }

  return reorderedPlans;
}
