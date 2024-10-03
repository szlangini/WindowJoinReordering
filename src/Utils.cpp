#include "Utils.h"

#include <iostream>
#include <random>

#include "WindowJoinOperator.h"

std::shared_ptr<Stream> createStream(
    const std::string& name, int numTuples,
    std::function<long(int, int)> valueDistribution, long maxTimestamp,
    int multiplicator) {
  auto stream = std::make_shared<Stream>(name);

  // Generate timestamps evenly spaced between 0 and maxTimestamp
  long timestampStep = maxTimestamp / (numTuples - 1);

  for (int i = 0; i < numTuples; ++i) {
    long value = valueDistribution(i, multiplicator);
    long timestamp = i * timestampStep;
    stream->addTuple({value}, timestamp);
  }

  return stream;
}

long linearValueDistribution(int index, int multiplicator) {
  return multiplicator * (index + 1);
}

long randomValueDistribution(int index, int multiplicator) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(1, 100);
  return multiplicator * dis(gen);
}

void printJoinKeyVector(const std::vector<JoinKey>& joinKeys) {
  for (const auto& joinKey : joinKeys) {
    std::cout << joinKey.toString() << std::endl;
  }
}

void printWindowAssignments(
    const std::unordered_map<JoinKey, std::vector<WindowSpecification>,
                             JoinKeyHash>& windowAssignments) {
  for (const auto& entry : windowAssignments) {
    std::cout << entry.first.toString() << " -> [";
    for (const auto& spec : entry.second) {
      std::cout << spec.toString()
                << ", ";  // Assuming WindowSpecification has a toString()
    }
    std::cout << "]" << std::endl;
  }
}
