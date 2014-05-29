#include <boost/uuid/uuid.hpp>  // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>  // streaming operators etc.
#include <sstream>

#include "helper/pipelining.h"

std::string getChunkIdentifier(std::string prefix) {
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  std::stringstream id;
  id << prefix << "_" << uuid;
  return id.str();
};
