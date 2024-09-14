// SlidingWindowJoin.h
#ifndef SLIDING_WINDOW_JOIN_H
#define SLIDING_WINDOW_JOIN_H

#include <memory>
#include <vector>

#include "Window.h"
#include "WindowJoinOperator.h"

class SlidingWindowJoin : public WindowJoinOperator {
 public:
  SlidingWindowJoin(std::shared_ptr<Stream> leftChild,
                    std::shared_ptr<Stream> rightChild, long length, long slide,
                    const std::string& timestampPropagator);

  ~SlidingWindowJoin();

  void createWindows() override;
  std::shared_ptr<Stream> compute() override;

  void eliminateDuplicates(std::vector<Tuple>& results);

 private:
  std::shared_ptr<Stream> leftChild;
  std::shared_ptr<Stream> rightChild;
  long length;
  long slide;

  std::vector<Window> windows;
};

#endif  // SLIDING_WINDOW_JOIN_H
