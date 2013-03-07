#include "access/MaterializingMainDeltaScan.h"

#include "helper/checked_cast.h"
#include "storage/PointerCalculator.h"
#include "storage/RawTable.h"
#include "storage/AbstractTable.h"

namespace hyrise { namespace access {

namespace {
auto _ = QueryParser::registerPlanOperation<MaterializingMainDeltaScan>("MaterializingMainDelta");
}

struct copy_raw_func {
  typedef void value_type;

  const std::shared_ptr<const RawTable<>>& _source;
  const storage::atable_ptr_t& _target;
  const size_t& _col;
  const size_t& _source_row;
  const size_t& _target_row;
  
  copy_raw_func(const std::shared_ptr<const RawTable<>>& source,
            const storage::atable_ptr_t& target,
            const size_t& column,
                const size_t& source_row,
                const size_t& target_row)
      : _source(source), _target(target), _col(column), _source_row(source_row), _target_row(target_row) {}
  
  template<typename R>
  void operator()() {
    _target->setValue<R>(_col, _target_row, _source->getValue<R>(_col, _source_row));
    }
};

void appendRows(const std::shared_ptr< Table<> >& target,
                size_t target_row_offset,
                const std::shared_ptr< const RawTable<> >& source,
                const pos_list_t& source_positions
                ) {
  hyrise::storage::type_switch<hyrise_basic_types> ts;
  const auto mdsize = target->metadata().size();

  for(size_t row=0, source_sz=source_positions.size(); row < source_sz; ++row) {
    const auto actual_row = target_row_offset + row;
    for(size_t column=0; column < mdsize; ++column) {
      copy_raw_func tf(source, target, column, source_positions[row], actual_row);
      ts(target->typeOfColumn(column), tf);
    }
  }
}


void MaterializingMainDeltaScan::executePlanOperation() {
  const auto& main_pc = checked_pointer_cast<const PointerCalculator>(input.getTable(0));
  const auto& delta_pc = checked_pointer_cast<const PointerCalculator>(input.getTable(1));

  const auto main_sz = main_pc->size();
  const auto delta_sz = delta_pc->size();

  auto result = checked_pointer_cast<Table<>>(main_pc->copy_structure(nullptr, false, 0, false));
  /*result->resize(main_sz + delta_sz);
  for (size_t row = 0, sz = main_sz; row < sz; ++row) {
    result->copyRowFrom(main_pc, row, row, true);
  }
  
  const auto& raw_delta = checked_pointer_cast< const RawTable<> >(delta_pc->getTable());
  const pos_list_t* raw_positions = delta_pc->getPositions();
  appendRows(result, main_sz, raw_delta, *raw_positions);
  output.add(result);*/
}

std::shared_ptr<_PlanOperation> MaterializingMainDeltaScan::parse(Json::Value&) {
  return std::make_shared<MaterializingMainDeltaScan>();
}

}}
