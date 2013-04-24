// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
  #ifndef SRC_LIB_ACCESS_PRED_BETWEENOPERATION_H_
#define SRC_LIB_ACCESS_PRED_BETWEENOPERATION_H_

#include "helper/types.h"
#include "pred_common.h"
#include "storage/RawTable.h"

template <typename T>
class BetweenExpression : public SimpleFieldExpression {
 private:
  value_id_t _lowerBound;
  value_id_t _upperBound;
  T lower_value;
  T upper_value;
  std::shared_ptr<BaseDictionary<T>> valueIdMap;
 public:
  BetweenExpression(size_t i, field_t f, T _lower_value, T _upper_value):
      SimpleFieldExpression(i, f), lower_value(_lower_value), upper_value(_upper_value)
  {}

  BetweenExpression(size_t i, field_name_t f, T _lower_value, T _upper_value):
      SimpleFieldExpression(i, f), lower_value(_lower_value), upper_value(_upper_value)
  {}

  BetweenExpression(hyrise::storage::c_atable_ptr_t _table, field_t _field, T _lower_value, T _upper_value) :
      SimpleFieldExpression(_table, _field), lower_value(_lower_value), upper_value(_upper_value)
  {}

  virtual void walk(const std::vector<hyrise::storage::c_atable_ptr_t > &l) {
    SimpleFieldExpression::walk(l);
    valueIdMap = std::dynamic_pointer_cast<BaseDictionary<T>>(table->dictionaryAt(field));
    assert(valueIdMap->isOrdered());
    _lowerBound = valueIdMap->getValueIdForValueSmaller(lower_value);
    _upperBound = valueIdMap->getValueIdForValueGreater(upper_value);
  }

  inline virtual bool operator()(size_t row) {
    ValueId valueId = table->getValueId(field, row);
    return (_lowerBound <= valueId.valueId) && (valueId.valueId < _upperBound);
  }
};

template <typename T>
class BetweenExpressionRaw : public SimpleFieldExpression {
 private:
  T lower_value;
  T upper_value;
 public:
  BetweenExpressionRaw(size_t i, field_t f, T _lower_value, T _upper_value):
      SimpleFieldExpression(i, f), lower_value(_lower_value), upper_value(_upper_value)
  {}

  BetweenExpressionRaw(size_t i, field_name_t f, T _lower_value, T _upper_value):
      SimpleFieldExpression(i, f), lower_value(_lower_value), upper_value(_upper_value)
  {}

  BetweenExpressionRaw(hyrise::storage::c_atable_ptr_t _table, field_t _field, T _lower_value, T _upper_value) :
      SimpleFieldExpression(_table, _field), lower_value(_lower_value), upper_value(_upper_value)
  {}

  inline virtual bool operator()(size_t row) {
    auto value = std::static_pointer_cast<const RawTable<>>(table)->getValue<T>(field, row);
    return (value <= upper_value) && (value >= lower_value);
  }
};
#endif  // SRC_LIB_ACCESS_PRED_BETWEENOPERATION_H_
