// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/IndexScan.h"

#include <memory>

#include "access/BasicParser.h"
#include "access/json_converters.h"
#include "access/QueryParser.h"

#include "io/StorageManager.h"

#include "storage/InvertedIndex.h"
#include "storage/meta_storage.h"
#include "storage/PointerCalculator.h"
#include "storage/PointerCalculatorFactory.h"

namespace hyrise { namespace access {

namespace {
bool reg_ix = QueryParser::registerPlanOperation<IndexScan>("IndexScan");
bool reg_ixr = QueryParser::registerPlanOperation<IndexRangeScan>("IndexRangeScan");
bool reg_mix = QueryParser::registerTrivialPlanOperation<MergeIndexScan>("MergeIndexScan");
}

struct ScanIndexFunctor {
  typedef pos_list_t *value_type;

  const std::shared_ptr<AbstractIndex>& _index;
  const Json::Value& _indexValue;

  ScanIndexFunctor(const Json::Value& value, std::shared_ptr<AbstractIndex> d):
    _index(d), _indexValue(value) {}

  template<typename ValueType>
  value_type operator()() {

    auto idx = std::dynamic_pointer_cast<InvertedIndex<ValueType> >(_index);
    auto v = json_converter::convert<ValueType>(_indexValue);
    pos_list_t *result = new pos_list_t(idx->getPositionsForKey(v));
    return result;
  }
};

void IndexScan::executePlanOperation() {
  StorageManager *sm = StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_indexName);

  // Handle type of index and value
  storage::type_switch<hyrise_basic_types> ts;
  ScanIndexFunctor fun(_value, idx);

  storage::pos_list_t *pos = ts(input.getTable(0)->typeOfColumn(_field_definition[0]), fun);

  addResult(PointerCalculatorFactory::createPointerCalculatorNonRef(input.getTable(0),
                                                                    nullptr,
                                                                    pos));
}

std::shared_ptr<_PlanOperation> IndexScan::parse(Json::Value &data) {
  std::shared_ptr<IndexScan> s = BasicParser<IndexScan>::parse(data);
  s->_value = data["value"];
  s->_indexName = data["index"].asString();
  return s;
}

struct RangeIndexFunctor {
  typedef pos_list_t value_type;

  const Json::Value& _from, & _to;
  const std::shared_ptr<AbstractIndex>& _index;

  RangeIndexFunctor(const Json::Value& from,
                    const Json::Value& to,
                    const std::shared_ptr<AbstractIndex>& d) :
      _from(from), _to(to), _index(d) {}

  template<typename ValueType>
  value_type operator()() {
    auto idx = std::dynamic_pointer_cast<InvertedIndex<ValueType> >(_index);
    auto fromValue = json_converter::convert<ValueType>(_from);
    auto toValue = json_converter::convert<ValueType>(_to);
    auto positions = idx->getPositionsForRange(fromValue, toValue);
    std::sort(std::begin(positions), std::end(positions));
    return positions;
  }
};

IndexRangeScan::IndexRangeScan() {}

IndexRangeScan::IndexRangeScan(std::string indexName, Json::Value from, Json::Value to) :
    _indexName(indexName), _value_from(from), _value_to(to) {}

void IndexRangeScan::executePlanOperation() {
  const auto& idx = StorageManager::getInstance()->getInvertedIndex(_indexName);

  storage::type_switch<hyrise_basic_types> ts;
  RangeIndexFunctor fun(_value_from, _value_to, idx);

  const auto& tbl = input.getTable(0);
  pos_list_t pos = ts(tbl->typeOfColumn(_field_definition[0]), fun);

  // Hello ugly
  auto pos_ptr = new pos_list_t(std::move(pos));
  output.add(PointerCalculatorFactory::createPointerCalculatorNonRef(tbl, nullptr, pos_ptr));
}

std::shared_ptr<_PlanOperation> IndexRangeScan::parse(Json::Value &data) {
  auto s = BasicParser<IndexRangeScan>::parse(data);
  s->_value_from = data["value_from"];
  s->_value_to = data["value_to"];
  s->_indexName = data["index"].asString();
  return s;
}

void MergeIndexScan::executePlanOperation() {
  auto left = std::dynamic_pointer_cast<const PointerCalculator>(input.getTable(0));
  auto right = std::dynamic_pointer_cast<const PointerCalculator>(input.getTable(1));

  storage::pos_list_t result(std::max(left->getPositions()->size(), right->getPositions()->size()));

  auto it = std::set_intersection(left->getPositions()->begin(),
                                  left->getPositions()->end(),
                                  right->getPositions()->begin(),
                                  right->getPositions()->end(),
                                  result.begin());

  auto tmp = PointerCalculatorFactory::createPointerCalculator(left->getActualTable(),
                                                               nullptr,
                                                               new storage::pos_list_t(result.begin(), it));
  addResult(tmp);
}

}
}

