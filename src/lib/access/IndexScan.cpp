#include <memory>

#include <access/BasicParser.h>
#include <access/IndexScan.h>
#include <access/json_converters.h>
#include <access/QueryParser.h>

#include <io/StorageManager.h>

#include <storage/AbstractIndex.h>
#include <storage/InvertedIndex.h>
#include <storage/meta_storage.h>
#include <storage/PointerCalculator.h>
#include <storage/PointerCalculatorFactory.h>


struct CreateIndexValueFunctor {
  typedef AbstractIndexValue *value_type;

  Json::Value &d;

  explicit CreateIndexValueFunctor(Json::Value &c): d(c) {}

  template<typename R>
  value_type operator()() {
    IndexValue<R> *v = new IndexValue<R>();
    v->value = json_converter::convert<R>(d["value"]);
    return v;
  }

};

struct ScanIndexFunctor {

  typedef pos_list_t *value_type;

  std::shared_ptr<AbstractIndex> _index;
  AbstractIndexValue *_indexValue;

  ScanIndexFunctor(AbstractIndexValue *i, std::shared_ptr<AbstractIndex> d):
    _index(d), _indexValue(i) {}

  template<typename ValueType>
  value_type operator()() {
    auto idx = std::dynamic_pointer_cast<InvertedIndex<ValueType> >(_index);
    auto v = dynamic_cast<IndexValue<ValueType>* >(_indexValue);
    pos_list_t *result = new pos_list_t(idx->getPositionsForKey(v->value));
    return result;
  }

};


bool IndexScan::is_registered = QueryParser::registerPlanOperation<IndexScan>();
bool MergeIndexScan::is_registered = QueryParser::registerPlanOperation<MergeIndexScan>();


void IndexScan::executePlanOperation() {
  StorageManager *sm = StorageManager::getInstance();
  auto idx = sm->getInvertedIndex(_indexName);

  // Handle type of index and value
  hyrise::storage::type_switch<hyrise_basic_types> ts;
  ScanIndexFunctor fun(_value, idx);
  pos_list_t *pos = ts(input.getTable(0)->typeOfColumn(_field_definition[0]), fun);

  // Add result
  addResult(PointerCalculatorFactory::createPointerCalculatorNonRef(input.getTable(0),
            nullptr,
            pos));
}

std::shared_ptr<_PlanOperation> IndexScan::parse(Json::Value &data) {
  std::shared_ptr<IndexScan> s = BasicParser<IndexScan>::parse(data);
  hyrise::storage::type_switch<hyrise_basic_types> ts;
  CreateIndexValueFunctor civf(data);
  s->_value = ts(data["vtype"].asUInt(), civf);
  s->_indexName = data["index"].asString();
  return s;
}

std::shared_ptr<_PlanOperation> MergeIndexScan::parse(Json::Value &data) {
  return BasicParser<MergeIndexScan>::parse(data);
}


void MergeIndexScan::executePlanOperation() {

  auto left = std::dynamic_pointer_cast<const PointerCalculator>(input.getTable(0));
  auto right = std::dynamic_pointer_cast<const PointerCalculator>(input.getTable(1));

  pos_list_t result(std::max(left->getPositions()->size(), right->getPositions()->size()));

  auto it = std::set_intersection(left->getPositions()->begin(), left->getPositions()->end(),
                        right->getPositions()->begin(), right->getPositions()->end(),
                        result.begin());


  auto tmp = PointerCalculatorFactory::createPointerCalculator(left->getActualTable(), nullptr, new pos_list_t(result.begin(), it)); 
  addResult(tmp);
}