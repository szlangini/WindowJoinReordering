// SlidingWindowJoin.h
#ifndef SLIDING_WINDOW_JOIN_H
#define SLIDING_WINDOW_JOIN_H

#include <memory>
#include <vector>

#include "WindowJoinOperator.h"

class SlidingWindowJoin : public WindowJoinOperator {
 public:
  SlidingWindowJoin(std::shared_ptr<Node> leftChild,
                    std::shared_ptr<Node> rightChild, long length, long slide,
                    const TimeDomain timeDomain,
                    const std::string& timestampPropagator = "NONE");

  ~SlidingWindowJoin();

  void createWindows(const std::shared_ptr<Stream>& leftStream,
                     const std::shared_ptr<Stream>& rightStream) override;

  std::shared_ptr<Stream> getOutputStream() override;

  std::shared_ptr<Stream> compute() override;

  void eliminateDuplicates(std::vector<Tuple>& results);

  long getLength();
  long getSlide();

  std::string getJoinType() const override;

 private:
  long length;
  long slide;

  std::vector<Window> windows;
};

#endif  // SLIDING_WINDOW_JOIN_H
