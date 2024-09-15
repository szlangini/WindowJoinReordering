#ifndef JOIN_PLAN_H
#define JOIN_PLAN_H

#include <memory>

#include "SlidingWindowJoin.h"
#include "TimeDomain.h"
#include "WindowJoinOperator.h"

class JoinPlan {
 public:
  JoinPlan(const std::shared_ptr<Node>& root);

  // Calls compute on the root node
  std::shared_ptr<Stream> compute();

  std::string getJoinOrder() const { return root->getName(); }

  std::shared_ptr<Node> getRoot() const { return root; }

  std::string toString() const;

  TimeDomain getTimeDomain() const;

 private:
  std::shared_ptr<Node> root;
};

#endif  // JOIN_PLAN_H