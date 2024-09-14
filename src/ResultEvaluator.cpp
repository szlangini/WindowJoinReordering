// ResultEvaluator.cpp
#include "ResultEvaluator.h"

#include <iostream>
#include <set>

#define DEBUG_MODE_RESULT_EVALUATOR 1

long ResultEvaluator::computeSum(const std::vector<Tuple>& results) {
  long totalSum = 0;
  for (const auto& tuple : results) {
    for (const auto& value : tuple.values) {
      totalSum += value;
    }
  }
  return totalSum;
}

bool ResultEvaluator::compareResults(const std::vector<Tuple>& result1,
                                     const std::vector<Tuple>& result2,
                                     std::ostream& os) {
  std::set<std::pair<std::vector<long>, long>> set1, set2;

  for (const auto& tuple : result1) {
    std::vector<long> sortedValues = tuple.values;
    std::sort(sortedValues.begin(), sortedValues.end());
    set1.insert({sortedValues, tuple.timestamp});
  }
  for (const auto& tuple : result2) {
    std::vector<long> sortedValues = tuple.values;
    std::sort(sortedValues.begin(), sortedValues.end());
    set2.insert({sortedValues, tuple.timestamp});
  }

  bool isEqual = (set1 == set2);

  if (!isEqual) {
#if DEBUG_MODE_RESULT_EVALUATOR
    os << "Result sets are not equal.\n";

    // Find tuples missing in result1
    for (const auto& tuple : set2) {
      if (set1.find(tuple) == set1.end()) {
        os << "Missing in result1: (";
        for (size_t i = 0; i < tuple.first.size(); ++i) {
          os << tuple.first[i];
          if (i < tuple.first.size() - 1) os << ", ";
        }
        os << "), Timestamp: " << tuple.second << "\n";
      }
    }

    // Find extra tuples in result1
    for (const auto& tuple : set1) {
      if (set2.find(tuple) == set2.end()) {
        os << "Extra in result1: (";
        for (size_t i = 0; i < tuple.first.size(); ++i) {
          os << tuple.first[i];
          if (i < tuple.first.size() - 1) os << ", ";
        }
        os << "), Timestamp: " << tuple.second << "\n";
      }
    }
#endif
  }

  return isEqual;
}
