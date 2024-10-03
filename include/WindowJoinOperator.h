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
    // Compare join types
    if (joinType != other.joinType) {
      return false;
    }

    // Compare unordered sets of left and right streams
    std::unordered_set<std::string> leftSet(leftStreams.begin(),
                                            leftStreams.end());
    std::unordered_set<std::string> rightSet(rightStreams.begin(),
                                             rightStreams.end());

    std::unordered_set<std::string> otherLeftSet(other.leftStreams.begin(),
                                                 other.leftStreams.end());
    std::unordered_set<std::string> otherRightSet(other.rightStreams.begin(),
                                                  other.rightStreams.end());

    return leftSet == otherLeftSet && rightSet == otherRightSet;
  }

  std::string joinTypeToString(JoinType joinType) const {
    switch (joinType) {
      case JoinType::SlidingWindowJoin:
        return "SlidingWindowJoin";
      case JoinType::IntervalJoin:
        return "IntervalJoin";
      // Add other cases if necessary
      default:
        return "UnknownJoinType";
    }
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "JoinKey(Left: {";
    for (const auto& stream : leftStreams) {
      ss << stream << ", ";
    }
    ss << "}, Right: {";
    for (const auto& stream : rightStreams) {
      ss << stream << ", ";
    }
    ss << "}, JoinType: " << joinTypeToString(joinType) << ")";
    return ss.str();
  }
};

struct JoinKeyHash {
  std::size_t operator()(const JoinKey& key) const {
    std::unordered_set<std::string> leftSet(key.leftStreams.begin(),
                                            key.leftStreams.end());
    std::unordered_set<std::string> rightSet(key.rightStreams.begin(),
                                             key.rightStreams.end());

    // Compute combined hash for both sets
    std::size_t leftHash = 0;
    std::size_t rightHash = 0;

    for (const auto& stream : leftSet) {
      leftHash ^= std::hash<std::string>{}(stream);
    }

    for (const auto& stream : rightSet) {
      rightHash ^= std::hash<std::string>{}(stream);
    }

    return leftHash ^ rightHash ^
           std::hash<int>{}(static_cast<int>(key.joinType));
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