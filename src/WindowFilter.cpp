// WindowFilter.cpp
#include "WindowFilter.h"

#include <algorithm>

WindowFilter::WindowFilter(const std::shared_ptr<Node>& child, long length,
                           long slide, const TimeDomain timeDomain)
    : Node("WindowFilter"),
      child(child),
      length(length),
      slide(slide),
      timeDomain(timeDomain) {}

void WindowFilter::createWindows(const std::shared_ptr<Stream>& inputStream) {
  // Determine the minimum and maximum timestamps from the input stream
  long minTimestamp = inputStream->getMinTimestamp();
  long maxTimestamp = inputStream->getMaxTimestamp();

  // Align windows with the logical clock, starting from minTimestamp
  for (long windowStart = minTimestamp; windowStart <= maxTimestamp;
       windowStart += slide) {
    long windowEnd = windowStart + length;
    windows.emplace_back(windowStart, windowEnd);
  }
}

std::shared_ptr<Stream> WindowFilter::compute() {
  auto inputStream = child->getOutputStream();
  std::vector<Tuple> filteredTuples;

  // Ensure windows are created
  if (windows.empty()) {
    createWindows(inputStream);
  }

  // Assign tuples to windows and collect filtered tuples
  for (const auto& tuple : inputStream->getTuples()) {
    for (const auto& window : windows) {
      if (tuple.getTimestamp() >= window.getStart() &&
          tuple.getTimestamp() < window.getEnd()) {
        filteredTuples.push_back(tuple);
        break;  // Once the tuple is assigned to a window, no need to check
                // further
      }
    }
  }

  // Create a new Stream with a name representing the filter
  std::string streamName = inputStream->getName() + "_Filtered";
  auto outputStream = std::make_shared<Stream>(streamName, false);

  // Set the base streams of the output stream
  outputStream->setBaseStreams(inputStream->getBaseStreams());

  // Add filtered tuples to the output stream
  for (const auto& tuple : filteredTuples) {
    outputStream->addTuple(tuple.values, tuple.timestamp);
  }

  return outputStream;
}

std::shared_ptr<Stream> WindowFilter::getOutputStream() { return compute(); }
