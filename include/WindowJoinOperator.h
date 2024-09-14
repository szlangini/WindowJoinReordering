// WindowJoinOperator.h
#ifndef WINDOW_JOIN_OPERATOR_H
#define WINDOW_JOIN_OPERATOR_H

#include <string>

#include "Stream.h"
#include "Tuple.h"

class WindowJoinOperator {
 public:
  WindowJoinOperator(const std::string& timestampPropagator);

  virtual ~WindowJoinOperator() = default;

  virtual void createWindows() = 0;
  virtual std::shared_ptr<Stream> compute() = 0;

 protected:
  std::string timestampPropagator;
};

#endif  // WINDOW_JOIN_OPERATOR_H