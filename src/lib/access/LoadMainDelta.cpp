#include "access/LoadMainDelta.h"

#include "storage/SimpleStore.h"
#include "io/StorageManager.h"
#include "io/Loader.h"
#include "io/CSVLoader.h"
#include "io/StringLoader.h"

namespace hyrise {
namespace access {

namespace {
auto _ = QueryParser::registerPlanOperation<LoadMainDelta>("LoadMainDelta");
}

LoadMainDelta::LoadMainDelta(const std::string& main, const std::string& delta, const std::string& header) : _main(main), _delta(delta), _header(header) {}

void LoadMainDelta::executePlanOperation() {
  std::unique_ptr<AbstractHeader> header;
  if (_header.empty()) {
    header.reset(new CSVHeader(_main));
  } else { header.reset(new StringHeader(_header)); }
  auto main = Loader::load(Loader::params()
                           .setBasePath(StorageManager::getInstance()->makePath(""))
                           .setHeader(*header)
                           .setInput(CSVInput(_main))
                           .setReturnsMutableVerticalTable(true));
  auto delta = Loader::load(Loader::params()
                            .setBasePath(StorageManager::getInstance()->makePath(""))
                            .setHeader(CSVHeader(_delta))
                            .setInput(CSVInput(_delta)));

  auto store = std::make_shared<storage::SimpleStore>(main);
  auto store_delta = store->getDelta();
  store_delta->appendRows(delta);
  output.add(store);
}

std::shared_ptr<_PlanOperation> LoadMainDelta::parse(Json::Value& value) {
  return std::make_shared<LoadMainDelta>(value["mainfile"].asString(), value["deltafile"].asString(), value["header"].asString());
}

}}
