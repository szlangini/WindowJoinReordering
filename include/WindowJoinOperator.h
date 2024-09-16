// WindowJoinOperator.h
#ifndef WINDOW_JOIN_OPERATOR_H
#define WINDOW_JOIN_OPERATOR_H

#include <string>

#include "Node.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "Tuple.h"
#include "Window.h"

class WindowJoinOperator : public Node {
 public:
  WindowJoinOperator(const std::shared_ptr<Node>& leftChild,
                     const std::shared_ptr<Node>& rightChild,
                     const TimeDomain timeDomain,
                     const std::string& timestampPropagator = "NONE");

  virtual ~WindowJoinOperator() = default;

  virtual void createWindows(const std::shared_ptr<Stream>& leftStream,
                             const std::shared_ptr<Stream>& rightStream) = 0;
  virtual std::shared_ptr<Stream> compute() = 0;

  std::shared_ptr<Stream> getOutputStream() override = 0;

  std::shared_ptr<Node> getLeftChild();
  std::shared_ptr<Node> getRightChild();

  const std::string& getTimestampPropagator();
  TimeDomain getTimeDomain() const;

  long determineTimestamp(const Window&, const Tuple& leftTuple,
                          const Tuple& rightTuple,
                          const std::shared_ptr<Stream>& leftStream,
                          const std::shared_ptr<Stream>& rightStream);

  // identify join type
  virtual std::string getJoinType() const = 0;

 protected:
  std::shared_ptr<Node> leftChild;
  std::shared_ptr<Node> rightChild;
  std::string timestampPropagator;
  TimeDomain timeDomain;
};

#endif  // WINDOW_JOIN_OPERATOR_H