#include "gtest/gtest.h"
#include "storage/DictionaryFactory.h"
#include "storage/OrderIndifferentDictionary.h"
#include "storage/OrderPreservingDictionary.h"

namespace hyrise {
namespace storage {

TEST(factory, creation) { auto d = makeDictionary(IntegerType, 10); }
}
}
