// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#ifndef SRC_LIB_ACCESS_PLANOPERATION_H_
#define SRC_LIB_ACCESS_PLANOPERATION_H_

#include "access/system/OutputTask.h"
#include "access/system/OperationData.h"
#include "access/system/QueryParser.h"
#include "io/TXContext.h"

#include "storage/storage_types.h"
#include "helper/types.h"
#include "storage/AbstractTable.h"

#include "json.h"


namespace hyrise {
namespace access {

class ResponseTask;

/**
 * This is the default interface for a plan operation. Our basic assumption is
 * that a plan operation has multiple input data structures and exactly one
 * output data structure.
 */
class PlanOperation : public OutputTask {
 protected:
  void addResult(storage::c_atable_ptr_t result);
  void addResultHash(storage::c_ahashtable_ptr_t result);

  virtual void computeDeferredIndexes();

  /*!
   *  Uses _part and _count member as specification for the enumeration of
   *  parallel instances to distribute the number of elements between all
   *  instances.
   */
  void distribute(
      const u_int64_t numberOfElements,
      u_int64_t &first,
      u_int64_t &last) const;

  /*!
   *  Fetches output data of dependencies as input data.
   */
  virtual void refreshInput();

  /*!
   *  If operator is supposed to be a parallel instance of an operator,
   *  separate input data based on instance enumeration.
   */
  virtual void splitInput();

  virtual void setupPlanOperation();
  virtual void executePlanOperation() = 0;
  virtual void teardownPlanOperation() {}

  /* Returns true when none of the dependencies have OpFail state */
  bool allDependenciesSuccessful();

  /* Returns all errors of dependencies as one concatenated std::string */
  std::string getDependencyErrorMessages();

 public:
  virtual ~PlanOperation();

  void setLimit(uint64_t l);
  void setProducesPositions(bool p);
  
  void setTXContext(tx::TXContext ctx);

  void addInput(storage::c_aresource_ptr_t t);

  const storage::c_atable_ptr_t getInputTable(size_t index = 0) const;
  const storage::c_atable_ptr_t getResultTable(size_t index = 0) const;
  storage::c_ahashtable_ptr_t getInputHashTable(size_t index = 0) const;
  storage::c_ahashtable_ptr_t getResultHashTable(size_t index = 0) const;

  void addField(field_t field);
  void addField(const Json::Value &field);
  void addNamedField(const field_name_t& field);

  void setPart(size_t part);
  void setCount(size_t count);
  void setPlanId(std::string i);
  void setOperatorId(std::string i);
  const std::string& planOperationName() const;
  void setPlanOperationName(const std::string& name);

  virtual void operator()() noexcept;
  virtual const std::string vname();
  const PlanOperation *execute();

  void setResponseTask(const std::shared_ptr<access::ResponseTask>& responseTask);
  std::shared_ptr<access::ResponseTask> getResponseTask() const;
 protected:

  /// Containers to store and handle input/output or rather result data.
  access::OperationData input;
  access::OperationData output;

  /// Limits the number of rows read
  uint64_t _limit = 0;

  /// Transaction number

  /// The fields used in the projection etc.
  field_list_t _field_definition;
  field_name_list_t _named_field_definition;
  field_list_t _indexed_field_definition;

  size_t _part = 0;
  size_t _count = 0;
  std::weak_ptr<access::ResponseTask> _responseTask;

  bool producesPositions = true;

  std::string _planId;
  std::string _operatorId;
  std::string _planOperationName;
  
  tx::TXContext _txContext;

};

}}

class _PlanOperation : public hyrise::access::PlanOperation {};

#endif  // SRC_LIB_ACCESS_PLANOPERATION_H_
