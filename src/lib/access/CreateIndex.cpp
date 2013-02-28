// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/CreateIndex.h"

#include <string>
#include <vector>
#include <map>

#include "access/BasicParser.h"
#include "access/QueryParser.h"
#include "io/StorageManager.h"
#include "storage/AbstractTable.h"
#include "storage/meta_storage.h"
#include "storage/storage_types.h"
#include "storage/PointerCalculator.h"
#include "storage/PointerCalculatorFactory.h"
#include "storage/AbstractIndex.h"
#include "storage/InvertedIndex.h"

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<CreateIndex>("CreateIndex");
}

std::shared_ptr<_PlanOperation> CreateIndex::parse(Json::Value &data) {
  auto i = BasicParser<CreateIndex>::parse(data);
  i->setTableName(data["table_name"].asString());
  return i;
}

template <typename T>
struct CreateIndexFunctor {
  typedef std::shared_ptr<AbstractIndex> value_type;

  const const_ptr_t<T>& in;

  size_t column;

  CreateIndexFunctor(const const_ptr_t<T>& t, size_t c):
    in(t), column(c) {}

  template<typename R>
  value_type operator()() {
    return std::make_shared<InvertedIndex<R>>(in, column);
  }
};


void CreateIndex::executePlanOperation() {
  const auto& in = input.getTable(0);
  std::shared_ptr<AbstractIndex> _index;

  auto column = _field_definition[0];

  CreateIndexFunctor<AbstractTable> fun(in, column);
  hyrise::storage::type_switch<hyrise_basic_types> ts;
  _index = ts(in->typeOfColumn(column), fun);

  StorageManager *sm = StorageManager::getInstance();
  sm->addInvertedIndex(_table_name, _index);
}

const std::string CreateIndex::vname() {
  return "CreateIndex";
}

void CreateIndex::setTableName(std::string t) {
  _table_name = t;
}

}
}
