// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_STORAGE_INVERTEDINDEX_H_
#define SRC_LIB_STORAGE_INVERTEDINDEX_H_

#include <vector>
#include <map>
#include <stdexcept>
#include <algorithm>

#include "helper/types.h"

#include "storage/storage_types.h"
#include "storage/AbstractIndex.h"
#include "storage/AbstractTable.h"
#include "storage/RawTable.h"
#include <memory>
    
template<typename T>
class InvertedIndex : public AbstractIndex {
private:
  typedef std::map<T, pos_list_t> inverted_index_t;
  inverted_index_t _index;

public:
  virtual ~InvertedIndex() {};

  void shrink() {
for (auto & e : _index)
      e.second.shrink_to_fit();
  }

  explicit InvertedIndex(const hyrise::storage::c_atable_ptr_t& in, field_t column) {
    if (in != NULL) {
      if (const auto& raw = std::dynamic_pointer_cast<const RawTable<>>(in)) {
        for (size_t row = 0; row < in->size(); ++row) {
          T tmp = raw->getValue<T>(column, row);
          typename inverted_index_t::iterator find = _index.find(tmp);
          if (find == _index.end()) {
            pos_list_t pos;
            pos.push_back(row);
            _index[tmp] = pos;
          } else {
            find->second.push_back(row);
          }
        }
      } else {
        for (size_t row = 0; row < in->size(); ++row) {
          T tmp = in->getValue<T>(column, row);
          typename inverted_index_t::iterator find = _index.find(tmp);
          if (find == _index.end()) {
            pos_list_t pos;
            pos.push_back(row);
            _index[tmp] = pos;
          } else {
            find->second.push_back(row);
          }
        }
      }
    }
  }

  /**
   * returns a list of positions where key was found.
   */
  const pos_list_t& getPositionsForKey(T key) const {
    const auto& it = _index.find(key);
    if (it != _index.end()) {
      return it->second;
    } else {
      static   pos_list_t empty;
      return empty;
    }
  };

  pos_list_t getPositionsForRange(const T& lower, const T& upper) const {
    assert((_index.key_comp()(lower, upper) == true) || (lower == upper));
    const auto& lb = _index.lower_bound(lower);
    const auto& ub = _index.upper_bound(upper);
    pos_list_t positions;
    for (auto it = lb; it != ub; ++it) {
      const auto& pos = it->second;
      positions.insert(std::end(positions), std::begin(pos), std::end(pos));
    }
    return positions;
  }
  
  
};
#endif  // SRC_LIB_STORAGE_INVERTEDINDEX_H_
