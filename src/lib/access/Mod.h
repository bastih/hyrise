// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_MOD_H_
#define SRC_LIB_ACCESS_MOD_H_

#include "access/system/ParallelizablePlanOperation.h"
#include "storage/storage_types.h"

namespace hyrise {
namespace access {

/// This class implements the mod function as a hyrise operation
/// You can set the divisor and the field that should be affected
/// This operation will create a new column containing the result values
/// Usage:
/// ........
///   "mod" :{
///       "type" : "Mod",
///       "fields" : ["C_ID"],
///       "vtype" : 0,
///       "divisor" : 10,
///       "as" : "MOD_FIELD"
///   },
/// ........
/// "fields" contains the field that acts as divident (should only have one element)
/// "vtype" should define the value type of the operation (int = 0, float = 1)
/// "divisor" is the number, that devides the given field
/// "as" contains the name of the new column

class Mod : public ParallelizablePlanOperation {
 public:
  Mod(float divisor, std::string sourceColName, std::string targetColName, DataType vtype)
      : _divisor(divisor), _sourceColName(sourceColName), _targetColName(targetColName), _vtype(vtype) {
    assert(divisor != 0.f);
  }

  void executePlanOperation();
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

 private:
  float _divisor = 1.f;
  std::string _sourceColName;
  std::string _targetColName;
  DataType _vtype = IntegerType;
};
}
}
#endif  // SRC_LIB_ACCESS_MOD_H_
