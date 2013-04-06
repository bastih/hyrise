#ifndef SRC_LIB_ACCESS_LOADMAINDELTA_H_
#define SRC_LIB_ACCESS_LOADMAINDELTA_H_

#include "access/PlanOperation.h"

namespace hyrise {
namespace access {

/* Constructs a Store with main and delta */
class LoadMainDelta : public _PlanOperation {
 public:
  LoadMainDelta(const std::string& mainfile, const std::string& delta, const std::string& header);
  void executePlanOperation();
  static std::shared_ptr<_PlanOperation> parse(Json::Value& data);
 private:
  const std::string _main;
  const std::string _delta;
  const std::string _header;
};

}}

#endif