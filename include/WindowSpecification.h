// WindowSpecification.h
#ifndef WINDOWSPECIFICATION_H
#define WINDOWSPECIFICATION_H

#include <functional>
struct WindowSpecification {
  enum class WindowType { SLIDING_WINDOW, INTERVAL_WINDOW, DEFAULT_WINDOW };

  WindowType type;
  long length;      // Used in SlidingWindowJoin
  long slide;       // Used in SlidingWindowJoin
  long lowerBound;  // Used in IntervalJoin
  long upperBound;  // Used in IntervalJoin

  WindowSpecification()
      : type(WindowType::DEFAULT_WINDOW),
        length(0),
        slide(0),
        lowerBound(0),
        upperBound(0) {}

 private:
  // Private constructor to enforce the use of factory methods
  WindowSpecification(WindowType t, long len, long sli, long lb, long ub)
      : type(t), length(len), slide(sli), lowerBound(lb), upperBound(ub) {}

 public:
  // Factory method for Sliding Window
  static WindowSpecification createSlidingWindowSpecification(long len,
                                                              long sli) {
    return WindowSpecification(WindowType::SLIDING_WINDOW, len, sli, 0, 0);
  }

  // Factory method for Interval Window
  static WindowSpecification createIntervalWindowSpecification(long lb,
                                                               long ub) {
    return WindowSpecification(WindowType::INTERVAL_WINDOW, 0, 0, lb, ub);
  }

  bool operator==(const WindowSpecification& other) const {
    return type == other.type && length == other.length &&
           slide == other.slide && lowerBound == other.lowerBound &&
           upperBound == other.upperBound;
  }

  // auto slidingSpec = WindowSpecification::createSlidingWindow(1000,500);
};

// Hash function for WindowSpecification
namespace std {
template <>
struct hash<WindowSpecification> {
  std::size_t operator()(const WindowSpecification& ws) const {
    // Compute individual hash values for each member and combine them
    std::size_t h1 = std::hash<int>{}(static_cast<int>(ws.type));
    std::size_t h2 = std::hash<long>{}(ws.length);
    std::size_t h3 = std::hash<long>{}(ws.slide);
    std::size_t h4 = std::hash<long>{}(ws.lowerBound);
    std::size_t h5 = std::hash<long>{}(ws.upperBound);

    // Combine all hash values (you can use other hash combination algorithms)
    return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3) ^ (h5 << 4);
  }
};
}  // namespace std
#endif  // WINDOWSPECIFICATION_H