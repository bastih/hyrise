#ifndef SRC_LIB_ACCESS_MERGEPOSITIONS_H_
#define SRC_LIB_ACCESS_MERGEPOSITIONS_H_

#include "access/PlanOperation.h"

namespace hyrise { namespace access {

class MergePositions : public _PlanOperation {
 public:
  virtual void executePlanOperation();
};

}}

#endif
