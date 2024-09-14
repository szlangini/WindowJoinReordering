// src/Stream.cpp

#include "Stream.h"

Stream::Stream(const std::string& name, bool isBaseStream) : name(name) {
  if (isBaseStream) {
    baseStreams.insert(
        name);  // Base streams initially contain the stream's own name
  }
}

Stream::Stream(const std::string& name, const std::vector<Tuple>& tuples,
               bool isBaseStream)
    : name(name), tuples(tuples) {
  if (isBaseStream) {
    baseStreams.insert(
        name);  // Base streams initially contain the stream's own name
  }
}

void Stream::addTuple(const std::vector<long>& values, long timestamp) {
  tuples.push_back({values, timestamp});
}

void Stream::addTuple(Tuple& tuple) { tuples.push_back(tuple); }

const std::vector<Tuple>& Stream::getTuples() const { return tuples; }

const std::string& Stream::getName() const { return name; }

long Stream::getMinTimestamp() const {
  //   if (tuples.empty()) return 0;
  //   return tuples.front().timestamp;
  return 0;  // Let's always start at 0.
}

long Stream::getMaxTimestamp() const {
  if (tuples.empty()) return 0;
  return tuples.back().timestamp;
}

const std::unordered_set<std::string>& Stream::getBaseStreams() const {
  return baseStreams;
}

void Stream::setBaseStreams(
    const std::unordered_set<std::string>& baseStreams) {
  this->baseStreams = baseStreams;
}