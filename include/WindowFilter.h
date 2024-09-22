// WindowFilter.h
#ifndef WINDOW_FILTER_H
#define WINDOW_FILTER_H

#include <memory>
#include <vector>

#include "Node.h"
#include "Stream.h"
#include "TimeDomain.h"
#include "Tuple.h"
#include "Window.h"

class WindowFilter : public Node {
 public:
  WindowFilter(const std::shared_ptr<Node>& child, long length, long slide,
               const TimeDomain timeDomain);

  std::shared_ptr<Stream> getOutputStream() override;

 private:
  std::shared_ptr<Node> child;
  long length;
  long slide;
  TimeDomain timeDomain;

  std::vector<Window> windows;

  void createWindows(const std::shared_ptr<Stream>& inputStream);
  std::shared_ptr<Stream> compute();
};

#endif  // WINDOW_FILTER_H
