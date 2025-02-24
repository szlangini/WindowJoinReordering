// include/Stream.h

#ifndef STREAM_H
#define STREAM_H

#include <string>
#include <unordered_set>
#include <vector>

#include "Node.h"
#include "Tuple.h"

class Stream : public Node {
 public:
  Stream(const std::string& name, bool isBaseStream = true, long rate = 1);

  Stream(const std::string& name, const std::vector<Tuple>& tuples,
         bool isBaseStream = true);

  void addTuple(const std::vector<long>& values, long timestamp);

  void addTuple(Tuple& tuple);

  const std::vector<Tuple>& getTuples() const;

  long getMinTimestamp() const;
  long getMaxTimestamp() const;

  long getRate() const;

  const std::unordered_set<std::string>& getBaseStreams() const;

  void setBaseStreams(const std::unordered_set<std::string>& baseStreams);

  std::shared_ptr<Stream> getOutputStream() override;

  void printTuples() const;

 private:
  std::vector<Tuple> tuples;
  std::unordered_set<std::string> baseStreams;
  long rate;
};

#endif  // STREAM_H
