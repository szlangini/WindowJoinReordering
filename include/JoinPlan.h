#ifndef JOIN_PLAN_H
#define JOIN_PLAN_H

#include <memory>

#include "WindowJoinOperator.h"

class JoinPlan {
 public:
  JoinPlan(const std::shared_ptr<Node>& root);

  // Calls compute on the root node
  std::shared_ptr<Stream> compute();

 private:
  std::shared_ptr<Node> root;
};

#endif  // JOIN_PLAN_H