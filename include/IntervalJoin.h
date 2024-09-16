#ifndef INTERVALJOIN_H
#define INTERVALJOIN_H

#include "WindowJoinOperator.h"

class IntervalJoin : public WindowJoinOperator {
 public:
  IntervalJoin(std::shared_ptr<Node> leftChild,
               std::shared_ptr<Node> rightChild, long lowerBound,
               long upperBound, const std::string& timestampPropagator);

  ~IntervalJoin();

  void createWindows(const std::shared_ptr<Stream>& leftStream,
                     const std::shared_ptr<Stream>& rightStream) override;

  std::shared_ptr<Stream> getOutputStream() override;

  std::shared_ptr<Stream> compute() override;

  const long getLowerBound() const;
  const long getUpperBound() const;

 private:
  long lowerBound;
  long upperBound;
  std::vector<Window> windows;
};

#endif  // INTERVALJOIN_H
