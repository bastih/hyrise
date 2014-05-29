#include <iostream>

#include "ebb/ebb.h"
#include "net/AsyncConnection.h"
#include "net/ShutdownHandler.h"

namespace hyrise {
namespace net {

bool ShutdownHandler::registered = Router::registerRoute<ShutdownHandler>("/shutdown/");


std::string ShutdownHandler::name() { return "ShutdownHandler"; }

const std::string ShutdownHandler::vname() { return "ShutdownHandler"; }

void ShutdownHandler::operator()() {
  if (auto ac = dynamic_cast<AsyncConnection*>(_connection)) {
    ac->respond("shutting down");
    ebb_server_unlisten(ac->connection->server);
  }
}
}
}
