#ifndef JOIN_ORDERER_H
#define JOIN_ORDERER_H

#include <algorithm>
#include <memory>
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

  // Get rid of unnecessary/illegal permutations given a JoinPlan
  bool prunePermutations(const std::shared_ptr<JoinPlan>& joinPlan);
};

#endif  // JOIN_ORDERER_H
