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

  bool operator==(const JoinKey& other) const {
    return joinType == other.joinType && leftStreams == other.leftStreams &&
           rightStreams == other.rightStreams;
  }
};

// Custom hash for JoinKey
struct JoinKeyHash {
  std::size_t operator()(const JoinKey& key) const {
    std::size_t h1 =
        std::hash<int>{}(static_cast<int>(key.joinType));  // Hash the enum
    std::size_t h2 = 0;
    std::size_t h3 = 0;

    // Hash the left streams
    for (const auto& stream : key.leftStreams) {
      h2 ^= std::hash<std::string>{}(stream);
    }

    // Hash the right streams
    for (const auto& stream : key.rightStreams) {
      h3 ^= std::hash<std::string>{}(stream);
    }

    return h1 ^ (h2 << 1) ^ (h3 << 2);  // Combine hashes
  }
};

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