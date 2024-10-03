// JoinPermutation.h
#ifndef JOINPERMUTATION_H
#define JOINPERMUTATION_H

#include "WindowJoinOperator.h"

class JoinPermutation {
 private:
  std::vector<JoinKey> steps;  // Holds the sequence of join steps

 public:
  // Constructor
  JoinPermutation() = default;

  // Add a new join step (represented by JoinKey)
  void addJoinStep(const JoinKey& joinKey) { steps.push_back(joinKey); }

  // Get the current join order as a vector of JoinKeys
  const std::vector<JoinKey>& getSteps() const { return steps; }

  // Compare two JoinPermutations (optional helper)
  bool operator==(const JoinPermutation& other) const {
    return steps == other.steps;
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "JoinPermutation: [";
    for (size_t i = 0; i < steps.size(); ++i) {
      ss << steps[i].toString();  // Assuming JoinKey has a toString method
      if (i != steps.size() - 1) {
        ss << " -> ";  // Separate each join step
      }
    }
    ss << "]";
    return ss.str();
  }
};

#endif  // JOINPERMUTATION_H
