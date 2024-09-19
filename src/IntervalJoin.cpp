#include "IntervalJoin.h"

#include <algorithm>
#include <memory>
#include <stdexcept>

#include "TimeDomain.h"

// Constructor
IntervalJoin::IntervalJoin(std::shared_ptr<Node> leftChild,
                           std::shared_ptr<Node> rightChild, long lowerBound,
                           long upperBound,
                           const std::string& timestampPropagator)
    : WindowJoinOperator(leftChild, rightChild, TimeDomain::EVENT_TIME,
                         timestampPropagator),
      lowerBound(lowerBound),
      upperBound(upperBound) {}

IntervalJoin::~IntervalJoin() {}

void IntervalJoin::createWindows(const std::shared_ptr<Stream>& leftStream,
                                 const std::shared_ptr<Stream>& rightStream) {
  auto counter = 0;
  for (const auto& tuple : leftStream->getTuples()) {
    // Span window from left tuple
    auto startTimestamp = std::max(tuple.getTimestamp() - lowerBound, 0l);
    auto endTimestamp = tuple.getTimestamp() + upperBound;

    windows.emplace_back(startTimestamp, endTimestamp);
    windows[counter].addLeftTuple(
        tuple);  // tuple that spans IntervalJoin is the only left tuple.

    ++counter;
  }

  // Assign right tuples to windows
  for (const auto& tuple : rightStream->getTuples()) {
    for (auto& window : windows) {
      if (tuple.getTimestamp() >= window.getStart() &&
          tuple.getTimestamp() <=
              window.getEnd()) {  // Windows are on both sides inclusive here!
        window.addRightTuple(tuple);
      }
    }
  }
}

std::shared_ptr<Stream> IntervalJoin::compute() {
  std::vector<Tuple> results;
  auto leftStream = leftChild->getOutputStream();
  auto rightStream = rightChild->getOutputStream();

  // Ensure windows are created
  if (windows.empty()) {
    createWindows(leftStream, rightStream);
  }

  // Perform join for each window
  for (const auto& window : windows) {
    const auto& leftTuples = window.getLeftTuples();
    const auto& rightTuples = window.getRightTuples();

    for (const auto& leftTuple : leftTuples) {
      for (const auto& rightTuple : rightTuples) {
        // Combine values
        std::vector<long> combinedValues = leftTuple.values;
        combinedValues.insert(combinedValues.end(), rightTuple.values.begin(),
                              rightTuple.values.end());

        long timestamp = determineTimestamp(window, leftTuple, rightTuple,
                                            leftStream, rightStream);
        // std::max(leftTuple.getTimestamp(), rightTuple.getTimestamp());

        // Create new tuple
        Tuple resultTuple = {combinedValues, timestamp};
        results.push_back(resultTuple);
      }
    }
  }

  std::string streamName = leftStream->getName() + "_" + rightStream->getName();
  auto outputStream = std::make_shared<Stream>(streamName, false);

  std::unordered_set<std::string> baseStreams = leftStream->getBaseStreams();
  baseStreams.insert(rightStream->getBaseStreams().begin(),
                     rightStream->getBaseStreams().end());
  outputStream->setBaseStreams(baseStreams);

  // Add tuples to the output stream
  for (const auto& tuple : results) {
    outputStream->addTuple(tuple.values, tuple.timestamp);
  }

  return outputStream;
}

std::shared_ptr<Stream> IntervalJoin::getOutputStream() { return compute(); }

const long IntervalJoin::getLowerBound() const { return lowerBound; }
const long IntervalJoin::getUpperBound() const { return upperBound; }

std::string IntervalJoin::getJoinType() const { return "IntervalJoin"; }