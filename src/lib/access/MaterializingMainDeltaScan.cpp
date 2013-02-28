#include "access/MaterializingMainDeltaScan.h"

#include "helper/checked_cast.h"
#include "storage/PointerCalculator.h"
#include "storage/RawTable.h"
#include "storage/AbstractTable.h"

namespace hyrise { namespace access {

struct copy_raw_func {
  typedef void value_type;

  const std::shared_ptr<const RawTable<>>& _source;
  const storage::atable_ptr_t& _target;
  const size_t& _col;
  const size_t& _row;
  
  copy_raw_func(const std::shared_ptr<const RawTable<>>& source,
            const storage::atable_ptr_t& target,
            const size_t& column,
            const size_t& row) : _source(source), _target(target), _col(column), _row(row) {}
  
  template<typename R>
  void operator()() {
    _target->setValue<R>(_col, _row, _source->getValue<R>(_col, _row));
    }
};

void appendRows(const std::shared_ptr< Table<> >& target,
                size_t target_row_offset,
                const std::shared_ptr< const RawTable<> >& source) {
  hyrise::storage::type_switch<hyrise_basic_types> ts;
  const auto mdsize = target->metadata().size();

  for(size_t row=0, source_sz=source->size(); row < source_sz; ++row) {
    const auto actual_row = target_row_offset + row;
    for(size_t column=0; column < mdsize; ++column) {
      copy_raw_func tf(source, target, column, actual_row);
      ts(target->typeOfColumn(column), tf);
    }
  }
}


void MaterializingMainDeltaScan::executePlanOperation() {

  const auto& main_pc = checked_pointer_cast<const PointerCalculator>(input.getTable(0));
  const auto& delta_pc = checked_pointer_cast<const PointerCalculator>(input.getTable(1));

  const auto main_sz = main_pc->size();
  const auto delta_sz = delta_pc->size();

  auto result = checked_pointer_cast<Table<>>(main_pc->copy_structure(nullptr, false, main_sz + delta_sz, false));
  
  for (size_t row = 0, sz = main_sz; row < sz; ++row) {
    result->copyRowFrom(main_pc, row, row, true);
  }

  const auto& raw_delta = checked_pointer_cast< const RawTable<> >(delta_pc->getTable());
  appendRows(result, main_sz, raw_delta);
  
}

std::shared_ptr<_PlanOperation> MaterializingMainDeltaScan::parse(Json::Value&) {
  return std::make_shared<MaterializingMainDeltaScan>();
}

}}
