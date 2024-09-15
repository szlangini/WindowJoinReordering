#include "JoinOrderer.h"

#include <sstream>

#include "SlidingWindowJoin.h"
#include "Stream.h"

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

// Function to reorder the joins and return new JoinPlans
std::vector<std::shared_ptr<JoinPlan>> JoinOrderer::reorder(
    const std::shared_ptr<JoinPlan>& joinPlan) {
  std::vector<std::shared_ptr<JoinPlan>> reorderedPlans;

  // Get the current join order from the root node's name (e.g., "A_B_C")
  std::string currentOrder = joinPlan->getJoinOrder();

  // Split the currentOrder string into individual stream names (e.g., "A_B_C"
  // becomes ["A", "B", "C"])
  std::vector<std::string> streams = splitJoinOrder(currentOrder);

  // Generate all possible permutations of the current order
  std::vector<std::vector<std::string>> permutations;
  generatePermutations(streams, permutations);

  // Create new join plans for each permutation, pruning invalid ones
  for (const auto& perm : permutations) {
    // Rebuild the join structure dynamically based on the permutation
    std::shared_ptr<SlidingWindowJoin> join;

    // Dynamically create streams based on the permutation order
    std::shared_ptr<Stream> stream1 = std::make_shared<Stream>(perm[0]);
    std::shared_ptr<Stream> stream2 = std::make_shared<Stream>(perm[1]);
    std::shared_ptr<Stream> stream3 = std::make_shared<Stream>(perm[2]);

    long length = 10;
    long slide = 10;  // Tumbling window

    // Rebuild the joins based on the current permutation
    auto join12 = std::make_shared<SlidingWindowJoin>(
        stream1, stream2, length, slide, stream1->getName());
    join = std::make_shared<SlidingWindowJoin>(join12, stream3, length, slide,
                                               stream1->getName());

    // Create a new JoinPlan for the reordered join
    auto newJoinPlan = std::make_shared<JoinPlan>(join);

    // Prune invalid join plans
    if (prunePermutations(newJoinPlan)) {
      reorderedPlans.push_back(newJoinPlan);
    }
  }

  return reorderedPlans;
}
