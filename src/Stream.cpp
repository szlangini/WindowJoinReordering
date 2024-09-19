// src/Stream.cpp

#include "Stream.h"

#include <iostream>

Stream::Stream(const std::string& name, bool isBaseStream) : Node(name) {
  if (isBaseStream) {
    baseStreams.insert(
        name);  // Base streams initially contain the stream's own name
  }
}

Stream::Stream(const std::string& name, const std::vector<Tuple>& tuples,
               bool isBaseStream)
    : Node(name), tuples(tuples) {
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

const std::unordered_set<std::string>& Stream::getBaseStreams() const {
  return baseStreams;
}

void Stream::setBaseStreams(
    const std::unordered_set<std::string>& baseStreams) {
  this->baseStreams = baseStreams;
}

std::shared_ptr<Stream> Stream::getOutputStream() {
  // Stream simply returns itself as the output
  return std::make_shared<Stream>(*this);
}

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