// src/Stream.cpp

#include "Stream.h"

#include <iostream>

Stream::Stream(const std::string& name, bool isBaseStream, double rate)
    : Node(name), rate(rate) {
  std::cout << "hyello";
  if (isBaseStream) {
    baseStreams.insert(
        name);  // Base streams initially contain the stream's own name
  }
  std::cout << "constructing" << name << ", " << this->getRate() << std::endl;
}

Stream::Stream(const std::string& name, const std::vector<Tuple>& tuples,
               bool isBaseStream)
    : Node(name), tuples(tuples) {
  std::cout << "is this called?";
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

long Stream::getMinTimestamp() const {
  //   if (tuples.empty()) return 0;
  //   return tuples.front().timestamp;
  return 0;  // Let's always start at 0.
}

long Stream::getMaxTimestamp() const {
  if (tuples.empty()) return 0;
  return tuples.back().timestamp;
}

double Stream::getRate() const { return this->rate; }

const std::unordered_set<std::string>& Stream::getBaseStreams() const {
  return baseStreams;
}

void Stream::setBaseStreams(
    const std::unordered_set<std::string>& baseStreams) {
  this->baseStreams = baseStreams;
}

std::shared_ptr<Stream> Stream::getOutputStream() { return shared_from_this(); }

void Stream::printTuples() const {
  if (tuples.empty()) {
    std::cout << "Empty Stream " << name << std::endl;
    return;
  }

  std::cout << "Stream " << name << ":" << std::endl;
  for (const auto& tuple : tuples) {
    std::cout << tuple.toString() << std::endl;
  }
}