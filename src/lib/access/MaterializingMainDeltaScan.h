#ifndef SRC_LIB_ACCES_MATERIALIZINGMAINDELTASCAN_H
#define SRC_LIB_ACCES_MATERIALIZINGMAINDELTASCAN_H

#include "access/PlanOperation.h"

namespace hyrise { namespace access {

class MaterializingMainDeltaScan : public _PlanOperation {
 public:
  void executePlanOperation();
  const std::string vname() { return "MaterializingMainDeltaScan"; }
  static std::shared_ptr<_PlanOperation> parse(Json::Value& data);
};

}}

#endif
