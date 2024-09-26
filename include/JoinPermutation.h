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
  std::vector<JoinKey> getSteps() const { return steps; }

  // Compare two JoinPermutations (optional helper)
  bool operator==(const JoinPermutation& other) const {
    return steps == other.steps;
  }
};

#endif  // JOINPERMUTATION_H
