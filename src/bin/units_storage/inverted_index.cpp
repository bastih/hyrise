#include "testing/test.h"

#include "storage/InvertedIndex.h"
#include "io/shortcuts.h"

using namespace hyrise;

namespace {
auto table = Loader::shortcuts::load("test/groupby_xs.tbl");
}

TEST(InvertedIndexTest, key) {
  InvertedIndex<hyrise_int_t> ii(table, 0);
  auto s2008 = ii.getPositionsForKey(2008).size();
  ASSERT_EQ(6, s2008); 
}

TEST(InvertedIndexTest, range) {
  InvertedIndex<hyrise_int_t> ii(table, 0);
  auto s2008 = ii.getPositionsForKey(2008).size();
  auto s2009 = ii.getPositionsForKey(2009).size();
  auto sCombined = ii.getPositionsForRange(2008, 2009).size();
  ASSERT_EQ(s2008+s2009, sCombined) << "the range should cover the same positions as its values";
}

TEST(InvertedIndexTest, range_single_value) {
  InvertedIndex<hyrise_int_t> ii(table, 0);
  auto s2008 = ii.getPositionsForKey(2008).size();
  auto s2008range = ii.getPositionsForRange(2008, 2008).size();
  ASSERT_EQ(s2008, s2008range) << "the range should cover the same positions as the value";
}

TEST(InvertedIndexTest, range_all) {
  InvertedIndex<hyrise_int_t> ii(table, 0);
  auto range = ii.getPositionsForRange(2008, 2012).size();
  ASSERT_EQ(table->size(), range) << "the range should cover the whole table";
}

TEST(InvertedIndexTest, range_outofbounds) {
  InvertedIndex<hyrise_int_t> ii(table, 0);
  auto range = ii.getPositionsForRange(2013, 2020).size();
  ASSERT_EQ(0, range) << "the range should be empty";
}
