#ifndef NODE_H
#define NODE_H

#include <memory>
#include <string>

class Stream;

class Node {
 public:
  explicit Node(const std::string& name) : name(name) {}

  virtual ~Node() = default;

  // Returns the output Stream (for Stream or WindowJoinOperator)
  virtual std::shared_ptr<Stream> getOutputStream() = 0;

  std::string getName() const { return name; }

 protected:
  std::string name;
};

#endif  // NODE_H
