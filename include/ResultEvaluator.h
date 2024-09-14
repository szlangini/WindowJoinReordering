// ResultEvaluator.h
#ifndef RESULT_EVALUATOR_H
#define RESULT_EVALUATOR_H

#include <ostream>
#include <vector>

#include "Tuple.h"

class ResultEvaluator {
 public:
  long computeSum(const std::vector<Tuple>& results);

  // Method to compare results and print discrepancies
  bool compareResults(const std::vector<Tuple>& result1,
                      const std::vector<Tuple>& result2,
                      std::ostream& os);  // Output stream for messages
};

#endif  // RESULT_EVALUATOR_H