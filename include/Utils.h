#ifndef UTILS_H
#define UTILS_H

#include <functional>
#include <memory>
#include <vector>

#include "Stream.h"

// Function declarations
std::shared_ptr<Stream> createStream(
    const std::string& name, int numTuples,
    std::function<long(int, int)> valueDistribution, long maxTimestamp,
    int multiplicator);

long linearValueDistribution(int index, int multiplicator);
long randomValueDistribution(int index, int multiplicator);

#endif  // UTILS_H