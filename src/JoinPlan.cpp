#include "JoinPlan.h"

#include <iostream>
#include <sstream>
#include <stdexcept>

#include "IntervalJoin.h"
#include "Node.h"
#include "WindowJoinOperator.h"

JoinPlan::JoinPlan(const std::shared_ptr<Node>& rootNode) : root(rootNode) {}

// Calls compute on the root node and returns the output stream
std::shared_ptr<Stream> JoinPlan::compute() { return root->getOutputStream(); }

// Helper function to recursively build the string representation of the plan

std::string buildJoinPlanString(const std::shared_ptr<Node>& node,
                                int depth = 0) {
  std::ostringstream oss;
  std::string indent(depth * 2, ' ');  // Indentation for hierarchy

  if (auto stream = std::dynamic_pointer_cast<Stream>(node)) {
    // Print the stream name if it's a Stream
    oss << indent << "Stream: " << stream->getName() << "\n";
  } else if (auto joinOp =
                 std::dynamic_pointer_cast<WindowJoinOperator>(node)) {
    // Use getJoinType() to determine the join type
    oss << indent << joinOp->getJoinType() << "(";
    if (joinOp->getJoinType() == "SlidingWindowJoin") {
      auto slidingJoin = std::dynamic_pointer_cast<SlidingWindowJoin>(joinOp);
      oss << "Length: " << slidingJoin->getLength()
          << ", Slide: " << slidingJoin->getSlide();
    } else if (joinOp->getJoinType() == "IntervalJoin") {
      auto intervalJoin = std::dynamic_pointer_cast<IntervalJoin>(joinOp);
      oss << "Lower Bound: " << intervalJoin->getLowerBound()
          << ", Upper Bound: " << intervalJoin->getUpperBound();
    }
    oss << ", Propagator: " << joinOp->getTimestampPropagator() << ")\n";

    // Recursively print left and right children
    oss << indent << "Left:\n"
        << buildJoinPlanString(joinOp->getLeftChild(), depth + 1);
    oss << indent << "Right:\n"
        << buildJoinPlanString(joinOp->getRightChild(), depth + 1);
  } else {
    // Handle unknown node types (shouldn't happen)
    oss << indent << "Unknown node type\n";
  }

  return oss.str();
}

// JoinPlan::toString() function
std::string JoinPlan::toString() const { return buildJoinPlanString(root); }

TimeDomain JoinPlan::getTimeDomain() const {
  // Try to cast the root node to WindowJoinOperator
  auto windowJoinOperator = std::dynamic_pointer_cast<WindowJoinOperator>(root);

  // If the cast succeeds, return the TimeDomain
  if (windowJoinOperator) {
    return windowJoinOperator->getTimeDomain();
  }

  // If the root node is not a WindowJoinOperator, throw an error
  throw std::runtime_error("Root node is not a WindowJoinOperator");
}

std::string JoinPlan::getTimestampPropagator() const {
  if (auto windowJoin = std::dynamic_pointer_cast<WindowJoinOperator>(root)) {
    return windowJoin->getTimestampPropagator();
  }
  throw std::runtime_error("Unknown node.");
}
