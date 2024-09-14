#ifndef NODE_H
#define NODE_H

#include <memory>

class Stream;

class Node {
 public:
  virtual ~Node() = default;

  // Returns the output Stream (for Stream or WindowJoinOperator)
  virtual std::shared_ptr<Stream> getOutputStream() = 0;
};

#endif  // NODE_H
