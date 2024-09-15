// WindowJoinOperator.cpp
#include "WindowJoinOperator.h"

WindowJoinOperator::WindowJoinOperator(const std::shared_ptr<Node>& leftChild,
                                       const std::shared_ptr<Node>& rightChild,
                                       const std::string& timestampPropagator)
    : Node(leftChild->getName() + "_" + rightChild->getName()),
      leftChild(leftChild),
      rightChild(rightChild),
      timestampPropagator(timestampPropagator) {}

std::shared_ptr<Node> WindowJoinOperator::getLeftChild() { return leftChild; }
std::shared_ptr<Node> WindowJoinOperator::getRightChild() { return rightChild; }

const std::string& WindowJoinOperator::getTimestampPropagator() {
  return timestampPropagator;
}
