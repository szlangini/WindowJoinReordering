// WindowJoinOperator.h
#ifndef WINDOW_JOIN_OPERATOR_H
#define WINDOW_JOIN_OPERATOR_H

#include <set>
#include <string>

#include "Node.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "Tuple.h"
#include "Window.h"
#include "WindowSpecification.h"

enum class JoinType { SlidingWindowJoin, IntervalJoin };

struct JoinKey {
  JoinType joinType;
  std::unordered_set<std::string> leftStreams;
  std::unordered_set<std::string> rightStreams;

  // Default constructor
  JoinKey() = default;

  // Parameterized constructor
  JoinKey(JoinType type, const std::unordered_set<std::string>& left,
          const std::unordered_set<std::string>& right)
      : joinType(type), leftStreams(left), rightStreams(right) {}

  // Equality operator
  bool operator==(const JoinKey& other) const {
    return joinType == other.joinType && leftStreams == other.leftStreams &&
           rightStreams == other.rightStreams;
  }
};

// Custom hash function for JoinKey
namespace std {
template <>
struct hash<JoinKey> {
  size_t operator()(const JoinKey& key) const {
    size_t h1 =
        std::hash<int>()(static_cast<int>(key.joinType));  // Hash join type
    size_t h2 = 0;

    // Hash the left streams
    for (const auto& stream : key.leftStreams) {
      h2 ^=
          std::hash<std::string>()(stream) + 0x9e3779b9 + (h2 << 6) + (h2 >> 2);
    }

    size_t h3 = 0;

    // Hash the right streams
    for (const auto& stream : key.rightStreams) {
      h3 ^=
          std::hash<std::string>()(stream) + 0x9e3779b9 + (h3 << 6) + (h3 >> 2);
    }

    return h1 ^ (h2 << 1) ^ (h3 << 1);  // Combine the hashes
  }
};
}  // namespace std

class WindowJoinOperator : public Node {
 public:
  WindowJoinOperator(const std::shared_ptr<Node>& leftChild,
                     const std::shared_ptr<Node>& rightChild,
                     const TimeDomain timeDomain,
                     const std::string& timestampPropagator = "NONE");

  virtual ~WindowJoinOperator() = default;

  virtual void createWindows(const std::shared_ptr<Stream>& leftStream,
                             const std::shared_ptr<Stream>& rightStream) = 0;
  virtual std::shared_ptr<Stream> compute() = 0;

  std::shared_ptr<Stream> getOutputStream() override = 0;

  std::shared_ptr<Node> getLeftChild();
  std::shared_ptr<Node> getRightChild();

  const std::string& getTimestampPropagator();
  TimeDomain getTimeDomain() const;

  long determineTimestamp(const Window&, const Tuple& leftTuple,
                          const Tuple& rightTuple,
                          const std::shared_ptr<Stream>& leftStream,
                          const std::shared_ptr<Stream>& rightStream);

  // identify join type
  virtual std::string getJoinType() const = 0;

 protected:
  std::shared_ptr<Node> leftChild;
  std::shared_ptr<Node> rightChild;
  std::string timestampPropagator;
  TimeDomain timeDomain;
};

#endif  // WINDOW_JOIN_OPERATOR_H