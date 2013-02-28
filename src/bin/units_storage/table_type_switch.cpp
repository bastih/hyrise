#include "testing/test.h"

#include "storage/meta_tables.h"

using namespace hyrise::storage;

struct functor {
  template <typename T>
  void operator()() {
    std::cout << sizeof(T) << std::endl;
  }
};

TEST(TableTypeSwitch, base) {
  
  table_type_switch<options<RawTable<>, Table<>>> ts;

  std::vector<const ColumnMetadata*> md;
  md.push_back(new ColumnMetadata("somename", IntegerType));
  auto r1 = new RawTable<>({}, 1);
  auto r2 = new Table<>(&md);
  
  functor func;
  ts(r1, func);
  ts(r2, func);
}


TEST(TableTypeSwitch, shared_ptr) {
  table_type_switch<options<RawTable<>, AbstractTable>, AbstractTable, shared_pointer_wrap> ts;

  std::vector<const ColumnMetadata*> md;
  md.push_back(new ColumnMetadata("somename", IntegerType));

  std::vector<ColumnMetadata> md2;
  md2.push_back(ColumnMetadata("somename", IntegerType));
  auto r1 = std::make_shared<RawTable<>>(md2, 1);
  auto r2 = std::make_shared<Table<>>(&md);
  
  functor func;
  ts(r1, func);
  ts(r2, func);
}
