#ifndef JOIN_ORDERER_H
#define JOIN_ORDERER_H

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "JoinPermutation.h"
#include "JoinPlan.h"
#include "WindowJoinOperator.h"
#include "WindowSpecification.h"

class JoinOrderer {
 public:
  // Reorders the join plan and returns a vector of JoinPlan (Algorithm 2)
  std::vector<std::shared_ptr<JoinPlan>> reorder(
      const std::shared_ptr<JoinPlan>& joinPlan);

  void generatePermutations(
      const std::vector<std::string>& streams,
      std::vector<std::vector<std::string>>& permutations);

  // Recursively gather all streams from the join tree
  void gatherStreams(
      const std::shared_ptr<Node>& node,
      std::unordered_map<std::string, std::shared_ptr<Stream>>& streamMap);

  std::pair<std::vector<WindowSpecification>,
            std::unordered_map<JoinKey, std::vector<WindowSpecification>>>
  getWindowSpecificationsAndAssignments(
      const std::shared_ptr<JoinPlan>& joinPlan);

  // returns all permutations there are without considering if they are
  // legal. This is in PT, hence only for Sliding Window Joins.
  std::vector<std::shared_ptr<JoinPlan>> getAllSlidingWindowJoinPermutations(
      const std::shared_ptr<JoinPlan>& joinPlan,
      const WindowSpecification generalWindowSpec);

  // For EVENT_TIME only, derives all possible Window Permutations (Algorithm 1)
  void deriveAllWindowPermutations(
      std::unordered_map<JoinKey, std::vector<WindowSpecification>>&
          windowAssignments);

  std::vector<JoinKey> decomposeJoinPair(const JoinKey& joinKey);

  void createCommutativePairs(
      std::unordered_map<JoinKey, std::vector<WindowSpecification>>&
          windowAssignments);

  std::vector<JoinPermutation> generateAllJoinPermutations(
      const std::shared_ptr<JoinPlan>& joinPlan);

  std::shared_ptr<JoinPlan> buildJoinPlanFromPermutation(
      const JoinPermutation& permutation,
      const std::unordered_map<JoinKey, std::vector<WindowSpecification>>&
          windowAssignments,
      const std::unordered_map<std::string, std::shared_ptr<Stream>>&
          streamMap);
};

#endif  // JOIN_ORDERER_H
