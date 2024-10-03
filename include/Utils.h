#ifndef UTILS_H
#define UTILS_H

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "Stream.h"
#include "WindowJoinOperator.h"
#include "WindowSpecification.h"

// Forward declarations
class JoinKey;
class WindowSpecification;
struct JoinKeyHash;

// Function declarations
std::shared_ptr<Stream> createStream(
    const std::string& name, int numTuples,
    std::function<long(int, int)> valueDistribution, long maxTimestamp,
    int multiplicator);

long linearValueDistribution(int index, int multiplicator);
long randomValueDistribution(int index, int multiplicator);

void printJoinKeyVector(const std::vector<JoinKey>& joinKeys);

void printWindowAssignments(
    const std::unordered_map<JoinKey, std::vector<WindowSpecification>,
                             JoinKeyHash>& windowAssignments);

#endif  // UTILS_H