#include "JoinPlan.h"

#include <sstream>

#include "SlidingWindowJoin.h"

JoinPlan::JoinPlan(const std::shared_ptr<Node>& rootNode) : root(rootNode) {}

// Calls compute on the root node and returns the output stream
std::shared_ptr<Stream> JoinPlan::compute() { return root->getOutputStream(); }

// Helper function to recursively build the string representation of the plan
std::string buildJoinPlanString(const std::shared_ptr<Node>& node,
                                int depth = 0) {
  std::ostringstream oss;
  std::string indent(depth * 2,
                     ' ');  // Indentation to make the hierarchy clearer

  if (auto stream = std::dynamic_pointer_cast<Stream>(node)) {
    // If the node is a Stream, just print the stream name
    oss << indent << "Stream: " << stream->getName() << "\n";
  } else if (auto join = std::dynamic_pointer_cast<SlidingWindowJoin>(node)) {
    // If the node is a SlidingWindowJoin, print its properties
    oss << indent << "SlidingWindowJoin(";
    oss << "Length: " << join->getLength() << ", Slide: " << join->getSlide();
    oss << ", Propagator: " << join->getTimestampPropagator() << ")\n";

    // Recursively print the left and right children
    oss << indent << "Left:\n"
        << buildJoinPlanString(join->getLeftChild(), depth + 1);
    oss << indent << "Right:\n"
        << buildJoinPlanString(join->getRightChild(), depth + 1);
  } else {
    // Handle unknown node types (should not happen)
    oss << indent << "Unknown node type\n";
  }

  return oss.str();
}

// JoinPlan::toString() function
std::string JoinPlan::toString() const { return buildJoinPlanString(root); }