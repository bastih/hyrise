// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_INDEX_SCAN
#define SRC_LIB_ACCESS_INDEX_SCAN

#include <string>
#include "access/PlanOperation.h"

#include "json.h"


namespace hyrise { namespace access {

/// Scan an existing index for the result. Currently only EQ predicates
/// allowed for the index.
class IndexScan : public _PlanOperation {
 public:
  void executePlanOperation();
  static std::shared_ptr<_PlanOperation> parse(Json::Value &data);
 private:
  /// index name
  std::string _indexName;
  /// value to compare with
  Json::Value _value;
};

class IndexRangeScan : public _PlanOperation {
 public:
  IndexRangeScan();
  IndexRangeScan(std::string indexName, Json::Value from, Json::Value to);
  void executePlanOperation();
  static std::shared_ptr<_PlanOperation> parse(Json::Value &data);
 private:
  /// index name
  std::string _indexName;
  /// values for range
  Json::Value _value_from, _value_to;
};
          
class MergeIndexScan : public _PlanOperation {
public:
  void executePlanOperation();
};


}}


#endif // SRC_LIB_ACCESS_INDEX_SCAN
