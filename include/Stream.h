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
  Stream(const std::string& name, bool isBaseStream = true);

  Stream(const std::string& name, const std::vector<Tuple>& tuples,
         bool isBaseStream = true);

  void addTuple(const std::vector<long>& values, long timestamp);

  void addTuple(Tuple& tuple);

  const std::vector<Tuple>& getTuples() const;

  const std::string& getName() const;

  long getMinTimestamp() const;
  long getMaxTimestamp() const;

  const std::unordered_set<std::string>& getBaseStreams() const;

  void setBaseStreams(const std::unordered_set<std::string>& baseStreams);

  std::shared_ptr<Stream> getOutputStream() override;

 private:
  std::string name;
  std::vector<Tuple> tuples;
  std::unordered_set<std::string> baseStreams;
};

#endif  // STREAM_H
