#ifndef JOIN_ORDERER_H
#define JOIN_ORDERER_H

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "JoinPlan.h"
#include "WindowSpecification.h"

class JoinOrderer {
 public:
  // Reorders the join plan and returns a vector of new JoinPlans
  std::vector<std::shared_ptr<JoinPlan>> reorder(
      const std::shared_ptr<JoinPlan>& joinPlan);

 private:
  void generatePermutations(
      const std::vector<std::string>& streams,
      std::vector<std::vector<std::string>>& permutations);

  // Recursively gather all streams from the join tree
  void gatherStreams(
      const std::shared_ptr<Node>& node,
      std::unordered_map<std::string, std::shared_ptr<Stream>>& streamMap);

  // Skip unnecessary/invalid plans
  bool isPruneablePlan(const std::string& firstPair,
                       const std::shared_ptr<Node>& root,
                       const std::vector<std::string>& perm,
                       std::set<std::string>& seenPairs, bool isSlidingWindow);

  // NEW STUFF
  std::pair<std::vector<WindowSpecification>,
            std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                               std::vector<WindowSpecification>>>
  getWindowSpecificationsAndAssignments(
      const std::shared_ptr<JoinPlan>& joinPlan);

  // Maps each WindowSpecification to the timestamp propagator.
  std::unordered_map<WindowSpecification, std::string> getTimestampPropagators(
      const std::shared_ptr<JoinPlan>& joinPlan,
      const std::vector<WindowSpecification>& windowSpecs);

  // returns all permutations there are without considering if they are
  // legal. This is in PT, hence only for Sliding Window Joins.
  std::vector<std::shared_ptr<JoinPlan>> getAllSlidingWindowJoinPermutations(
      const std::shared_ptr<JoinPlan>& joinPlan,
      const WindowSpecification generalWindowSpec);

  // Function to create updated window assignments for EVENT_TIME
  void createUpdatedWindowAssignments(
      std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                         std::vector<WindowSpecification>>& windowAssignments,
      const std::unordered_map<WindowSpecification, std::string>&
          timePropagators);

  std::vector<std::shared_ptr<WindowJoinOperator>> decomposeJoinPair(
      const std::shared_ptr<WindowJoinOperator>& joinOperator);

  void createCommutativePairs(
      std::unordered_map<std::shared_ptr<WindowJoinOperator>,
                         std::vector<WindowSpecification>>& windowAssignments)
};

#endif  // JOIN_ORDERER_H
