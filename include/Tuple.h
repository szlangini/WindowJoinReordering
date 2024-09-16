// include/Tuple.h

#ifndef TUPLE_H
#define TUPLE_H

#include <sstream>
#include <vector>

/**
 * Represents a data tuple used in stream processing.
 *
 * Members:
 * - `values`: A vector of long integers holding the data values.
 *   - If `values` contains a single element, it represents a tuple from a
 * single data stream.
 *   - If `values` contains multiple elements, it represents a tuple resulting
 * from a join operation between multiple streams.
 * - `timestamp`: A long integer representing the time associated with the
 * tuple.
 *   - This could be the time of creation, reception, or processing of the
 * tuple.
 *
 * Usage:
 * This struct is utilized in scenarios where data tuples need to be managed,
 * whether they originate from individual streams or are intermediate results
 * from stream joins. The `timestamp` aids in temporal operations, ordering, or
 * windowing functions in stream processing.
 */
struct Tuple {
  std::vector<long> values;
  long timestamp;

  const long getTimestamp() const { return timestamp; }
  const std::vector<long> getValues() const { return values; }

  const std::string toString() const {
    std::stringstream os;
    if (values.size() > 0) {
      os << '(';
    }
    for (size_t i = 0; i < values.size(); ++i) {
      os << values[i];
      if (i < values.size() - 1) os << ", ";
    }
    os << "), Timestamp: " << timestamp << "\n";
    return os.str();
  }
};

#endif  // TUPLE_H
