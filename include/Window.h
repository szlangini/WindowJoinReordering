// In Window.h
#include <vector>

#include "Tuple.h"

class Window {
 public:
  Window(long start, long end);

  void addLeftTuple(const Tuple& tuple);
  void addRightTuple(const Tuple& tuple);

  const std::vector<Tuple>& getLeftTuples() const;
  const std::vector<Tuple>& getRightTuples() const;

  long getStart() const;
  long getEnd() const;

  bool operator==(const Window& other) const;

 private:
  long start;
  long end;
  std::vector<Tuple> leftTuples;
  std::vector<Tuple> rightTuples;
};
