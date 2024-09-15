// WindowJoinOperator.cpp
#include "WindowJoinOperator.h"

#include <sstream>

#include "TimeDomain.h"

namespace {
// Helper function to convert base streams set to string
std::string baseStreamsToString(
    const std::unordered_set<std::string>& baseStreams) {
  std::ostringstream oss;
  oss << "{";
  for (auto it = baseStreams.begin(); it != baseStreams.end(); ++it) {
    oss << *it;
    if (std::next(it) != baseStreams.end()) {
      oss << ", ";
    }
  }
  oss << "}";
  return oss.str();
}
}  // namespace
WindowJoinOperator::WindowJoinOperator(const std::shared_ptr<Node>& leftChild,
                                       const std::shared_ptr<Node>& rightChild,
                                       const TimeDomain timeDomain,
                                       const std::string& timestampPropagator)
    : Node(leftChild->getName() + "_" + rightChild->getName()),
      leftChild(leftChild),
      rightChild(rightChild),
      timestampPropagator(timestampPropagator),
      timeDomain(timeDomain) {}

std::shared_ptr<Node> WindowJoinOperator::getLeftChild() { return leftChild; }
std::shared_ptr<Node> WindowJoinOperator::getRightChild() { return rightChild; }

const std::string& WindowJoinOperator::getTimestampPropagator() {
  return timestampPropagator;
}

TimeDomain WindowJoinOperator::getTimeDomain() const { return timeDomain; }

long WindowJoinOperator::determineTimestamp(
    const Window& window, const Tuple& leftTuple, const Tuple& rightTuple,
    const std::shared_ptr<Stream>& leftStream,
    const std::shared_ptr<Stream>& rightStream) {
  if (timeDomain == TimeDomain::PROCESSING_TIME) {
    return window.getEnd();
  } else if (leftStream->getBaseStreams().count(timestampPropagator) > 0) {
    return leftTuple.timestamp;
  } else if (rightStream->getBaseStreams().count(timestampPropagator) > 0) {
    return rightTuple.timestamp;
  } else {
    std::string leftChildBaseStreamsStr =
        baseStreamsToString(leftStream->getBaseStreams());
    std::string rightChildBaseStreamsStr =
        baseStreamsToString(rightStream->getBaseStreams());

    throw std::runtime_error("Timestamp propagator '" + timestampPropagator +
                             "' not found in base streams of either child.\n"
                             "Left child base streams: [" +
                             leftChildBaseStreamsStr +
                             "]\n"
                             "Right child base streams: [" +
                             rightChildBaseStreamsStr + "]");
  }
}
