// WindowSpecification.h
#ifndef WINDOWSPECIFICATION_H
#define WINDOWSPECIFICATION_H

#include <functional>
#include <string>
struct WindowSpecification {
  enum class WindowType { SLIDING_WINDOW, INTERVAL_WINDOW, DEFAULT_WINDOW };

  WindowType type;
  long length;      // Used in SlidingWindowJoin
  long slide;       // Used in SlidingWindowJoin
  long lowerBound;  // Used in IntervalJoin
  long upperBound;  // Used in IntervalJoin
  std::string timestampPropagator = "";
  // Todo: TimePropagator

  WindowSpecification()
      : type(WindowType::DEFAULT_WINDOW),
        length(0),
        slide(0),
        lowerBound(0),
        upperBound(0),
        timestampPropagator("NONE") {}

 private:
  // Private constructor to enforce the use of factory methods
  WindowSpecification(WindowType t, long len, long sli, long lb, long ub,
                      std::string timestampPropagator)
      : type(t),
        length(len),
        slide(sli),
        lowerBound(lb),
        upperBound(ub),
        timestampPropagator(timestampPropagator) {}

 public:
  // Factory method for Sliding Window
  static WindowSpecification createSlidingWindowSpecification(
      long len, long sli, std::string timestampPropagator) {
    return WindowSpecification(WindowType::SLIDING_WINDOW, len, sli, 0, 0,
                               timestampPropagator);
  }

  // Factory method for Interval Window
  static WindowSpecification createIntervalWindowSpecification(
      long lb, long ub, std::string timestampPropagator) {
    return WindowSpecification(WindowType::INTERVAL_WINDOW, 0, 0, lb, ub,
                               timestampPropagator);
  }

  bool operator==(const WindowSpecification& other) const {
    return type == other.type && length == other.length &&
           slide == other.slide && lowerBound == other.lowerBound &&
           upperBound == other.upperBound &&
           timestampPropagator == other.timestampPropagator;
  }
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
    // std::size_t h6 = std::hash<std::string>{}(ws.timestampPropagator); //
    // TODO: What todo if timestamps are different?

    // Combine all hash values (you can use other hash combination algorithms)
    return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3) ^ (h5 << 4);  //^ (h6 << 5);
  }
};
}  // namespace std

#endif  // WINDOWSPECIFICATION_H