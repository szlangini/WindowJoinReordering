// WindowJoinOperator.cpp
#include "WindowJoinOperator.h"

WindowJoinOperator::WindowJoinOperator(const std::shared_ptr<Node>& leftChild,
                                       const std::shared_ptr<Node>& rightChild,
                                       const std::string& timestampPropagator)
    : leftChild(leftChild),
      rightChild(rightChild),
      timestampPropagator(timestampPropagator) {}