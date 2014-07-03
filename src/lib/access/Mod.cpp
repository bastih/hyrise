// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "access/Mod.h"

#include "access/system/BasicParser.h"
#include "access/system/QueryParser.h"

#include "storage/AbstractTable.h"
#include "storage/MutableVerticalTable.h"
#include "storage/meta_storage.h"
#include "storage/storage_types.h"
#include "storage/TableBuilder.h"

#include <cmath>

namespace hyrise {
namespace access {

namespace {
auto _ = QueryParser::registerPlanOperation<Mod>("Mod");
}

void requireJsonKeys(const Json::Value& data, const std::initializer_list<const char*>& fields) {
  for (auto& f : fields)
    if (!data.isMember(f))
      throw std::runtime_error(std::string() + "'" + f + "' is missing");
}

const std::string& stringForType(DataType dt) {
  if (dt == IntegerType)
    return types::integer_name;
  if (dt == FloatType)
    return types::float_name;
  throw std::runtime_error("mssing dt");
}

struct ModFunctor {
  using value_type = void;
  storage::atable_ptr_t source, target;
  size_t source_col;
  float divisor;

  hyrise_int_t mod(hyrise_int_t value, float divisor) { return value % static_cast<hyrise_int_t>(divisor); }
  hyrise_float_t mod(hyrise_float_t value, float divisor) { return std::fmod(value, divisor); }

  template <typename T>
  void operator()() {
    for (size_t row = 0, e = source->size(); row < e; row++) {
      target->setValue<T>(0, row, mod(source->getValue<T>(source_col, row), divisor));
    }
  }
};

void Mod::executePlanOperation() {
  auto in = std::const_pointer_cast<storage::AbstractTable>(input.getTable(0));
  storage::TableBuilder::param_list list;
  list.append().set_type(stringForType(_vtype)).set_name(_targetColName);
  auto resultTable = storage::TableBuilder::build(list);
  resultTable->resize(in->size());
  ModFunctor mf{in, resultTable, in->numberOfColumn(_sourceColName), _divisor};
  // Limit type switch to integer and float (0, 1)
  storage::type_switch<boost::mpl::vector<hyrise_int_t, hyrise_float_t>>()(_vtype, mf);
  addResult(std::make_shared<storage::MutableVerticalTable>(std::vector<storage::atable_ptr_t>{in, resultTable}));
}

std::shared_ptr<PlanOperation> Mod::parse(const Json::Value& data) {
  requireJsonKeys(data, {"fields", "divisor", "as", "vtype"});

  if (data["fields"].size() != 1)
    throw std::runtime_error("Exactly one field in 'fields' required");

  if (data["vtype"].asInt() != 0 and data["vtype"].asInt() != 1)
    throw std::runtime_error("vtype has to be 0 or 1 for int or float");

  return std::make_shared<Mod>(data["divisor"].asFloat(),
                               data["fields"][0].asString(),
                               data["as"].asString(),
                               static_cast<DataType>(data["vtype"].asUInt()));
}
}
}
