#ifndef JOIN_ORDERER_H
#define JOIN_ORDERER_H

#include <algorithm>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "JoinPlan.h"

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
};

#endif  // JOIN_ORDERER_H
