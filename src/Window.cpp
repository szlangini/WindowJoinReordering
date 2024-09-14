// Window.cpp
#include "Window.h"

Window::Window(long start, long end) : start(start), end(end) {}

void Window::addLeftTuple(const Tuple& tuple) { leftTuples.push_back(tuple); }

void Window::addRightTuple(const Tuple& tuple) { rightTuples.push_back(tuple); }

const std::vector<Tuple>& Window::getLeftTuples() const {
  return leftTuples;
}

const std::vector<Tuple>& Window::getRightTuples() const {
  return rightTuples;
}

long Window::getStart() const { return start; }

long Window::getEnd() const { return end; }

bool Window::operator==(const Window& other) const {
  return (start == other.start) && (end == other.end);
}
