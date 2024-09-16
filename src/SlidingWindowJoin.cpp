// SlidingWindowJoin.cpp
#include "SlidingWindowJoin.h"

#include <algorithm>
#include <set>
#include <sstream>
#include <stdexcept>

#include "TimeDomain.h"

SlidingWindowJoin::SlidingWindowJoin(std::shared_ptr<Node> leftChild,
                                     std::shared_ptr<Node> rightChild,
                                     long length, long slide,
                                     const TimeDomain timeDomain,
                                     const std::string& timestampPropagator)
    : WindowJoinOperator(leftChild, rightChild, timeDomain,
                         timestampPropagator),
      length(length),
      slide(slide) {}

SlidingWindowJoin::~SlidingWindowJoin() {}

void SlidingWindowJoin::createWindows(
    const std::shared_ptr<Stream>& leftStream,
    const std::shared_ptr<Stream>& rightStream) {
  // Determine the minimum and maximum timestamps from both streams
  long minTimestamp = std::min(leftStream->getMinTimestamp(),
                               rightStream->getMinTimestamp());  // default is 0
  long maxTimestamp =
      std::max(leftStream->getMaxTimestamp(), rightStream->getMaxTimestamp());

  // Align windows with the logical clock, starting from minTimestamp
  for (long windowStart = minTimestamp; windowStart <= (maxTimestamp - length);
       windowStart += slide) {
    long windowEnd = windowStart + length;
    windows.emplace_back(windowStart, windowEnd);
  }

  // Assign left tuples to windows
  for (const auto& tuple : leftStream->getTuples()) {
    for (auto& window : windows) {
      if (tuple.getTimestamp() >= window.getStart() &&
          tuple.getTimestamp() < window.getEnd()) {
        window.addLeftTuple(tuple);
      }
    }
  }

  // Assign right tuples to windows
  for (const auto& tuple : rightStream->getTuples()) {
    for (auto& window : windows) {
      if (tuple.getTimestamp() >= window.getStart() &&
          tuple.getTimestamp() < window.getEnd()) {
        window.addRightTuple(tuple);
      }
    }
  }
}

std::shared_ptr<Stream> SlidingWindowJoin::compute() {
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

        // Create new tuple
        Tuple resultTuple = {combinedValues, timestamp};
        results.push_back(resultTuple);
      }
    }
  }

  eliminateDuplicates(results);

  // Create a new Stream with a name representing the join
  std::string streamName = leftStream->getName() + "_" + rightStream->getName();
  auto outputStream = std::make_shared<Stream>(streamName, false);

  // Set the base streams of the output stream to be the union of the base
  // streams of left and right children
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

// TODO: Jszlang ignore the timestamp part.
void SlidingWindowJoin::eliminateDuplicates(std::vector<Tuple>& results) {
  std::set<std::pair<std::vector<long>, long>> uniqueTuples;

  for (const auto& tuple : results) {
    uniqueTuples.insert({tuple.values, tuple.timestamp});
  }

  results.clear();
  for (const auto& item : uniqueTuples) {
    results.push_back({item.first, item.second});
  }
}

std::shared_ptr<Stream> SlidingWindowJoin::getOutputStream() {
  return compute();
}

long SlidingWindowJoin::getLength() { return length; }
long SlidingWindowJoin::getSlide() { return slide; };

std::string SlidingWindowJoin::getJoinType() const {
  return "SlidingWindowJoin";
}
