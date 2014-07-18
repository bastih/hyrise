// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "storage/HorizontalTable.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <iterator>

namespace hyrise {
namespace storage {

static std::vector<size_t> offsetsFromParts(const std::vector<c_atable_ptr_t>& parts) {
  std::vector<size_t> offsets(parts.size());
  auto total_size = 0;
  size_t i = 0;
  for (const auto& part : parts) {
    offsets[i++] = total_size;
    total_size += part->size();
  }
  return offsets;
}


static std::vector<table_id_t> tableIdOffsets(const std::vector<c_atable_ptr_t>& parts) {
  std::vector<table_id_t> offsets(parts.size() + 1, 0);
  std::transform(std::begin(parts), std::end(parts), std::begin(offsets) + 1, [](const c_atable_ptr_t& part) {
    return part->subtableCount();
  });
  std::partial_sum(std::begin(offsets), std::end(offsets), std::begin(offsets));
  return offsets;
}

HorizontalTable::HorizontalTable(std::vector<c_atable_ptr_t> parts)
    : _parts(parts), _offsets(offsetsFromParts(_parts)), _table_id_offsets(tableIdOffsets(parts)) {
  assert(_parts.size() != 0);
}

HorizontalTable::~HorizontalTable() = default;

inline size_t HorizontalTable::partForRow(const size_t row) const {
  auto r = std::find_if(std::begin(_offsets), std::end(_offsets), [=](size_t offset) { return offset > row; });
  return std::distance(std::begin(_offsets), r) - 1;
}



const ColumnMetadata& HorizontalTable::metadataAt(const size_t column_index, const size_t row) const {
  auto part = partForRow(row);
  return _parts[part]->metadataAt(column_index, row - _offsets[part]);
}

AbstractTable::cpart_t HorizontalTable::getPart(std::size_t column, std::size_t row) const {
  auto part = partForRow(row);
  return _parts[part]->getPart(column, row - _offsets[part]);
}

const adict_ptr_t& HorizontalTable::dictionaryAt(const size_t column, const size_t row) const {
  size_t part = partForRow(row);
  return _parts[part]->dictionaryAt(column, row - _offsets[part]);
}

void HorizontalTable::setDictionaryAt(adict_ptr_t dict, const size_t column, const size_t row) {
  throw std::runtime_error("Cannot set dictionary for HorizontalTable");
}

size_t HorizontalTable::size() const { return computeSize(); }

size_t HorizontalTable::columnCount() const { return _parts[0]->columnCount(); }

ValueId HorizontalTable::getValueId(const size_t column, const size_t row) const {
  size_t part = partForRow(row);
  ValueId valueId = _parts[part]->getValueId(column, row - _offsets[part]);
  // valueId.table = _table_id_offsets[part] + valueId.table;
  return valueId;
}

void HorizontalTable::setValueId(const size_t column, const size_t row, const ValueId valueId) {
  throw std::runtime_error("Cannot set ValueId for HorizontalTable");
}

unsigned HorizontalTable::partitionCount() const { return _parts[0]->partitionCount(); }

table_id_t HorizontalTable::subtableCount() const { return _table_id_offsets.back(); }

size_t HorizontalTable::partitionWidth(const size_t slice) const { return _parts[0]->partitionWidth(slice); }

atable_ptr_t HorizontalTable::copy() const { throw std::runtime_error("Not implemented"); }

void HorizontalTable::persist_scattered(const pos_list_t& elements, bool new_elements) const {
  for (const auto& p : _parts) {
    p->persist_scattered(elements, new_elements);
  }
}

size_t HorizontalTable::computeSize() const {
  return std::accumulate(
      _parts.begin(), _parts.end(), 0, [](size_t r, const c_atable_ptr_t& t) { return r + t->size(); });
}

void HorizontalTable::collectParts(std::list<cpart_t>& parts, size_t col_offset, size_t row_offset) const {
  for (auto& part : _parts) {
    part->collectParts(parts, col_offset, row_offset);
    row_offset += part->size();
  }
}

Visitation HorizontalTable::accept(StorageVisitor& visitor) const {
  if (visitor.visitEnter(*this) == Visitation::next) {
    for (auto& c : _parts) {
      if (c->accept(visitor) == Visitation::skip) {
        break;
      }
    }
  }
  return visitor.visitLeave(*this);
}

Visitation HorizontalTable::accept(MutableStorageVisitor& visitor) {
  if (visitor.visitEnter(*this) == Visitation::next) {
    for (auto& part : _parts) {
      auto c = std::const_pointer_cast<AbstractTable>(part);
      if (c->accept(visitor) == Visitation::skip) {
        break;
      }
    }
  }
  return visitor.visitLeave(*this);
}

}
}
