#include "access/MergePositions.h"

#include "helper/checked_cast.h"
#include "storage/PointerCalculator.h"

namespace hyrise { namespace access {

namespace { auto _ = QueryParser::registerTrivialPlanOperation<MergePositions>("MergePositions"); }

void MergePositions::executePlanOperation() {
  storage::c_calc_ptr_t pc = nullptr;
  if (input.numberOfTables() == 0) {
    throw std::runtime_error("MergePositions: Need at least one table in input");
  }
  for (const auto& input_table: input.getTables()) {
    const auto& input_pc = checked_pointer_cast<const PointerCalculator>(input_table);
    pc = (pc == nullptr ? input_pc : pc->intersect(input_pc) );

  }
  output.add(pc);
}

}}
