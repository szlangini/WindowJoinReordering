// utils.h
#ifndef UTILS_H
#define UTILS_H

#include <functional>
#include <memory>
#include <vector>

#include "Stream.h"

// Function to generate a Stream with automatic value distribution and
// timestamps
// Use multiplicator to scale values of different streams to different values.
std::shared_ptr<Stream> createStream(
    const std::string& name, int numTuples,
    std::function<long(int, int)> valueDistribution, long maxTimestamp,
    int multiplicator) {
  auto stream = std::make_shared<Stream>(name);

  // Generate timestamps evenly spaced between 0 and maxTimestamp
  long timestampStep = maxTimestamp / (numTuples - 1);

  for (int i = 0; i < numTuples; ++i) {
    long value = valueDistribution(i, multiplicator);
    long timestamp = i * timestampStep;
    stream->addTuple({value}, timestamp);
  }

  return stream;
}

#endif  // UTILS_H
